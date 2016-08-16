/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.pipeline

import akka.actor.ActorSystem
import akka.stream.{ActorAttributes, Attributes, KillSwitches, UniqueKillSwitch}
import akka.stream.scaladsl.{Flow, Keep, RunnableGraph, Sink, Source}
import akka.{Done, NotUsed}
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.sqs.AmazonSQSClient
import org.zalando.znap.config._
import org.zalando.znap.source.nakadi.objects.EventBatch
import org.zalando.znap.source.nakadi.{NakadiPublisher, NakadiTokens}
import org.zalando.znap.persistence.OffsetReaderSync
import org.zalando.znap.persistence.disk.{DiskEventsWriter, DiskOffsetReader, DiskOffsetWriter}
import org.zalando.znap.persistence.dynamo.{DynamoDBEventsWriter, DynamoDBOffsetReader, DynamoDBOffsetWriter}
import org.zalando.znap.signalling.sqs.SqsSignaller

import scala.concurrent.Future
import scala.util.control.NonFatal

private class PipelineBuilder(tokens: NakadiTokens)(actorSystem: ActorSystem) {

  private type FlowType = Flow[EventBatch, EventBatch, NotUsed]

  private lazy val diskExecutionContext = actorSystem.dispatchers.lookup(Config.Akka.DynamoDBDispatcher)
  private lazy val dynamoExecutionContext = actorSystem.dispatchers.lookup(Config.Akka.DynamoDBDispatcher)
//  private lazy val sqsExecutionContext = actorSystem.dispatchers.lookup(Config.Akka.SqsDispatcher)

  private var dynamoDBMap = Map.empty[String, DynamoDB]
  private def getDynamoDB(uri: String): DynamoDB = {
    dynamoDBMap.get(uri) match {
      case Some(dynamoDB) =>
        dynamoDB
      case _ =>
        val dynamoDBClient = new AmazonDynamoDBClient()
        dynamoDBClient.withEndpoint(uri)
        val dynamoDB = new DynamoDB(dynamoDBClient)
        dynamoDBMap = dynamoDBMap + (uri -> dynamoDB)
        dynamoDB
    }
  }

  private lazy val sqsClient = new AmazonSQSClient()

  /**
    * Build a pipeline in a form of Akka Streams graph.
    * @param id the pipeline ID.
    * @param pipelineInstanceId the id for a particular instance of the pipeline.
    * @param snapshotTarget snapshot target to be handled by the pipeline.
    */
  def build(id: String, pipelineInstanceId: String, snapshotTarget: SnapshotTarget): RunnableGraph[(UniqueKillSwitch, Future[PipelineResult])] = {
    val offsetReader = buildOffsetReader(snapshotTarget)
    val source = buildSource(snapshotTarget.source, offsetReader)
    val filterByEventClassStep = buildFilterByEventClassStep(snapshotTarget.source)
    val dataWriteStep = buildDataWriteStep(snapshotTarget)
    val signalStep = buildSignalStep(snapshotTarget)
    val offsetWriteStep = buildOffsetWriteStep(snapshotTarget)

    val sink = Sink.ignore
//    val sink = Sink.foreach(println)

    source
      .via(filterByEventClassStep)
      .via(dataWriteStep)
      .via(signalStep)
      .via(offsetWriteStep)
      .viaMat(KillSwitches.single)(Keep.right)
      .toMat(sink)(Keep.both)
      .named(s"Pipeline $id-$pipelineInstanceId")

      .mapMaterializedValue { case (killSwitch, f) =>
        implicit val ec = actorSystem.dispatcher

        val mappedFuture = f.map { case Done =>
          PipelineFinished(id, pipelineInstanceId)
        }
        .recover {
          case NonFatal(ex) => PipelineFailed(id, pipelineInstanceId, ex)
        }
        (killSwitch, mappedFuture)
      }
  }

  private def buildOffsetReader(snapshotTarget: SnapshotTarget): OffsetReaderSync = {
    val offsetReader = snapshotTarget.destination match {
      case dynamoDBDestination: DynamoDBDestination =>
        val uri = dynamoDBDestination.uri.toString
        new DynamoDBOffsetReader(snapshotTarget, getDynamoDB(uri))(dynamoExecutionContext)

      case _: DiskDestination =>
        new DiskOffsetReader(snapshotTarget)(diskExecutionContext)
    }

    offsetReader.init()
    offsetReader
  }

  private def buildSource(source: SnapshotSource, offsetReader: OffsetReaderSync): Source[EventBatch, Any] = {
    source match {
      case nakadiSource: NakadiSource =>
        val nakadiPublisher = new NakadiPublisher(nakadiSource, tokens, offsetReader)(actorSystem)
        nakadiPublisher.getSource()
    }
  }

  private def buildFilterByEventClassStep(source: SnapshotSource): FlowType = {
    source match {
      case nakadiSource: NakadiSource =>
        Flow[EventBatch].map { eventBatch =>
          val filteredEvents = eventBatch.events.map { eventList =>
            eventList.filter(e => e.eventClass == nakadiSource.eventClass)
          }
          EventBatch(eventBatch.cursor, filteredEvents)
        }.async
    }
  }

  private def buildDataWriteStep(snapshotTarget: SnapshotTarget): FlowType = {
    val (eventsWriter, dispatcherAttributes) = snapshotTarget.destination match {
      case dynamoDBDestination: DynamoDBDestination =>
        val uri = dynamoDBDestination.uri.toString
        (new DynamoDBEventsWriter(snapshotTarget, getDynamoDB(uri)),
          ActorAttributes.dispatcher(Config.Akka.DynamoDBDispatcher))

      case _: DiskDestination =>
        (new DiskEventsWriter(snapshotTarget),
          ActorAttributes.dispatcher(Config.Akka.DiskDBDispatcher))
    }

    eventsWriter.init()

    Flow[EventBatch].map { batch =>
      eventsWriter.write(batch.events.getOrElse(Nil))
      batch
    }
      .addAttributes(dispatcherAttributes)
      .async
  }

  private def buildSignalStep(snapshotTarget: SnapshotTarget): FlowType = {
    snapshotTarget.signalling match {
      case Some(sqsSignalling: SqsSignalling) =>
        val signaller = new SqsSignaller(sqsSignalling, sqsClient)

        Flow[EventBatch].map { batch =>
          val valuesToSignal = batch.events.getOrElse(Nil).map { event =>
            val key = snapshotTarget.key.foldLeft(event.body) { case (agg, k) =>
              agg.get(k)
            }.asText()
            key
          }

          signaller.signal(valuesToSignal)
          batch
        }
          .addAttributes(ActorAttributes.dispatcher(Config.Akka.SqsDispatcher))
          .async

      case None =>
        Flow[EventBatch].map(x => x) // empty
    }
  }

  private def buildOffsetWriteStep(snapshotTarget: SnapshotTarget): FlowType = {
    val (offsetWriter, dispatcherAttributes) = snapshotTarget.destination match {
      case dynamoDBDestination: DynamoDBDestination =>
        val uri = dynamoDBDestination.uri.toString
        (new DynamoDBOffsetWriter(snapshotTarget, getDynamoDB(uri)),
          ActorAttributes.dispatcher(Config.Akka.DynamoDBDispatcher))

      case _: DiskDestination =>
        (new DiskOffsetWriter(snapshotTarget),
          ActorAttributes.dispatcher(Config.Akka.DiskDBDispatcher))
    }

    offsetWriter.init()

    Flow[EventBatch].map { batch =>
      offsetWriter.write(batch.cursor)
      batch
    }
      .addAttributes(dispatcherAttributes)
      .async
  }
}
