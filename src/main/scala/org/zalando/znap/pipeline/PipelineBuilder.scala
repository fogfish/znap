/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.pipeline

import akka.actor.ActorSystem
import akka.stream.{KillSwitches, UniqueKillSwitch}
import akka.stream.scaladsl.{Flow, Keep, RunnableGraph, Sink, Source}
import akka.{Done, NotUsed}
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.sqs.AmazonSQSClient
import org.zalando.znap.config._
import org.zalando.znap.nakadi.objects.EventBatch
import org.zalando.znap.nakadi.{NakadiPublisher, NakadiTokens}
import org.zalando.znap.persistence.OffsetReader
import org.zalando.znap.persistence.dynamo.{DynamoDBEventsPersistor, DynamoDBOffsetPersistor, DynamoDBOffsetReader}
import org.zalando.znap.signalling.sqs.SqsSignaller

import scala.concurrent.Future
import scala.util.control.NonFatal

private class PipelineBuilder(tokens: NakadiTokens)(actorSystem: ActorSystem) {

  private type FlowType = Flow[EventBatch, EventBatch, NotUsed]

  private lazy val dynamoExecutionContext = actorSystem.dispatchers.lookup(Config.Akka.DynamoDBDispatcher)
  private lazy val sqsExecutionContext = actorSystem.dispatchers.lookup(Config.Akka.SqsDispatcher)

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

  def build(id: String, pipelineInstanceId: String, snapshotTarget: SnapshotTarget): RunnableGraph[(UniqueKillSwitch, Future[PipelineResult])] = {
    val offsetReader = buildOffsetReader(snapshotTarget)
    val source = buildSource(snapshotTarget.source, offsetReader)
    val filterByEventClassStep = buildFilterByEventClassStep(snapshotTarget.source)
    val dataPersistStep = buildDataPersistStep(snapshotTarget)
    val signalStep = buildSignalStep(snapshotTarget)
    val offsetPersistStep = buildOffsetPersistStep(snapshotTarget)

    val sink = Sink.ignore
//    val sink = Sink.foreach(println)

    source
      .via(filterByEventClassStep)
      .via(dataPersistStep)
      .via(signalStep)
      .via(offsetPersistStep)
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

  private def buildOffsetReader(snapshotTarget: SnapshotTarget): OffsetReader = {
    snapshotTarget.destination match {
      case dynamoDBDestination: DynamoDBDestination =>
        val uri = dynamoDBDestination.uri.toString
        new DynamoDBOffsetReader(snapshotTarget, getDynamoDB(uri))(dynamoExecutionContext)
    }
  }

  private def buildSource(source: SnapshotSource, offsetReader: OffsetReader): Source[EventBatch, Any] = {
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

  private def buildDataPersistStep(snapshotTarget: SnapshotTarget): FlowType = {
    snapshotTarget.destination match {
      case dynamoDBDestination: DynamoDBDestination =>
        implicit val ec = actorSystem.dispatcher

        val uri = dynamoDBDestination.uri.toString
        val dynamoDBEventsPersistor =
          new DynamoDBEventsPersistor(snapshotTarget, getDynamoDB(uri))(dynamoExecutionContext)

        Flow[EventBatch].mapAsync(1) { batch =>
          dynamoDBEventsPersistor.persist(batch.events.getOrElse(Nil))
            .map(_ => batch)
        }.async

    }
  }

  private def buildSignalStep(snapshotTarget: SnapshotTarget): FlowType = {
    snapshotTarget.signalling match {
      case Some(sqsSignalling: SqsSignalling) =>
        implicit val ec = actorSystem.dispatcher

        val signaller = new SqsSignaller(sqsSignalling, sqsClient)(sqsExecutionContext)

        Flow[EventBatch].mapAsync(1) { batch =>
          val valuesToSignal = batch.events.getOrElse(Nil).map { event =>
            val key = snapshotTarget.key.foldLeft(event.body) { case (agg, k) =>
              agg.get(k)
            }.asText()
            key
          }

          signaller.signal(valuesToSignal)
            .map(_ => batch)
        }.async

      case None =>
        Flow[EventBatch].map(x => x) // empty
    }
  }

  private def buildOffsetPersistStep(snapshotTarget: SnapshotTarget): FlowType = {
    snapshotTarget.destination match {
      case dynamoDBDestination: DynamoDBDestination =>
        implicit val ec = actorSystem.dispatcher

        val uri = dynamoDBDestination.uri.toString
        val offsetPersistor =
          new DynamoDBOffsetPersistor(snapshotTarget, getDynamoDB(uri))(dynamoExecutionContext)
        Flow[EventBatch].mapAsync(1) { batch =>
          offsetPersistor.persist(batch.cursor).map(_ => batch)
        }.async
    }
  }
}
