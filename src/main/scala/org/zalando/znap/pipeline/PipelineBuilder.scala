/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.pipeline

import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.{ActorAttributes, KillSwitches}
import akka.{Done, NotUsed}
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.kinesis.producer.{KinesisProducer, KinesisProducerConfiguration}
import com.amazonaws.services.sqs.AmazonSQSClient
import org.slf4j.LoggerFactory
import org.zalando.znap.PartitionId
import org.zalando.znap.config._
import org.zalando.znap.persistence.OffsetReaderSync
import org.zalando.znap.persistence.disk.{DiskEventsWriter, DiskOffsetReader, DiskOffsetWriter}
import org.zalando.znap.persistence.dynamo.{DynamoDBEventsWriter, DynamoDBOffsetReader, DynamoDBOffsetWriter}
import org.zalando.znap.pipeline.PipelineBuilder.EventBatchCountLogger
import org.zalando.znap.signalling.kinesis.KinesisSignaller
import org.zalando.znap.signalling.sqs.SqsSignaller
import org.zalando.znap.source.nakadi.objects.EventBatch
import org.zalando.znap.source.nakadi.{NakadiPublisher, NakadiTokens}
import org.zalando.znap.utils.{Compressor, Json}

import scala.util.control.NonFatal

private class PipelineBuilder(tokens: NakadiTokens)(actorSystem: ActorSystem) {

  private type FlowType = Flow[EventBatch, EventBatch, NotUsed]

  private val Encoding = "UTF-8"

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

  private var kinesisProducerMap = Map.empty[String, KinesisProducer]
  private def getKinesisProducer(amazonRegion: String): KinesisProducer = {
    kinesisProducerMap.get(amazonRegion) match {
      case Some(kinesisProducer) =>
        kinesisProducer
      case _ =>
        val config = new KinesisProducerConfiguration
        config.setRegion(amazonRegion)
        val kinesisProducer = new KinesisProducer(config)
        kinesisProducerMap += amazonRegion -> kinesisProducer
        kinesisProducer
    }
  }

  /**
    * Build a pipeline in a form of Akka Streams graph.
    * @param id the pipeline ID.
    * @param snapshotTarget snapshot target to be handled by the pipeline.
    */
  def build(id: String, snapshotTarget: SnapshotTarget): List[(String, Pipeline)] = {
    val offsetReader = buildOffsetReader(snapshotTarget)
    val sources = buildSource(snapshotTarget.source, offsetReader)

    sources.map { case (partitionId, source) =>
      val pipelineUniqueId = UUID.randomUUID().toString

      val filterByEventClassStep = buildFilterByEventClassStep(snapshotTarget.source)
      val dataWriteStep = buildDataWriteStep(snapshotTarget)
      val signalStep = buildSignalStep(snapshotTarget)
      val offsetWriteStep = buildOffsetWriteStep(snapshotTarget)

//      val sink = Sink.ignore
//      val sink = Sink.foreach(println)

      val eventBatchCountLogger = new EventBatchCountLogger(id, partitionId)
      val sink = Sink.foreach(eventBatchCountLogger.log)

      val graph = source
        .via(filterByEventClassStep)
        .via(dataWriteStep)
        .via(signalStep)
        .via(offsetWriteStep)
        .viaMat(KillSwitches.single)(Keep.right)
        .toMat(sink)(Keep.both)
        .named(s"Pipeline $id-partition$partitionId (unique ID $pipelineUniqueId)")

        .mapMaterializedValue { case (killSwitch, f) =>
          implicit val ec = actorSystem.dispatcher

          val mappedFuture = f.map { case Done =>
            PipelineFinished(id, partitionId)
          }
            .recover {
              case NonFatal(ex) => PipelineFailed(id, partitionId, ex)
            }
          (killSwitch, mappedFuture)
        }

      (partitionId, graph)
    }
  }

  private def buildOffsetReader(snapshotTarget: SnapshotTarget): OffsetReaderSync = {
    val offsetReader = snapshotTarget.destination match {
      case dynamoDBDestination: DynamoDBDestination =>
        val uri = dynamoDBDestination.uri.toString
        new DynamoDBOffsetReader(snapshotTarget, getDynamoDB(uri))(dynamoExecutionContext)

      case _: DiskDestination =>
        new DiskOffsetReader(snapshotTarget)(diskExecutionContext)

      case EmptyDestination =>
        throw new Exception("EmptyDestination is not supported")
    }

    offsetReader.init()
    offsetReader
  }

  private def buildSource(source: SnapshotSource, offsetReader: OffsetReaderSync): List[(PartitionId, Source[EventBatch, Any])] = {
    source match {
      case nakadiSource: NakadiSource =>
        val nakadiPublisher = new NakadiPublisher(nakadiSource, tokens, offsetReader)(actorSystem)
        nakadiPublisher.createSources()

      case EmptySource =>
        throw new Exception("EmptySource is not supported")
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

      case EmptySource =>
        throw new Exception("EmptySource is not supported")
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

      case EmptyDestination =>
        throw new Exception("EmptyDestination is not supported")
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
        buildSqsSignalStep(sqsSignalling, snapshotTarget.key)

      case Some(kinesisSignalling: KinesisSignalling) =>
        buildKinesisSignalStep(kinesisSignalling, snapshotTarget.key)

      case None =>
        Flow[EventBatch].map(x => x) // empty
    }
  }

  private def buildSqsSignalStep(sqsSignalling: SqsSignalling, keyPath: List[String]): FlowType = {
    val signaller = new SqsSignaller(sqsSignalling, sqsClient)

    Flow[EventBatch].map { batch =>
      val valuesToSignal = batch.events.getOrElse(Nil).map { event =>
        sqsSignalling.publishType match {
          case PublishType.KeysOnly =>
            Json.getKey(keyPath, event.body)

          case PublishType.EventsUncompressed =>
            Json.write(event)

          case PublishType.EventsCompressed =>
            Compressor.compressBase64(Json.write(event))
        }
      }
      signaller.signal(valuesToSignal)
      batch
    }
      .addAttributes(ActorAttributes.dispatcher(Config.Akka.SqsDispatcher))
      .async
  }

  private def buildKinesisSignalStep(kinesisSignalling: KinesisSignalling, keyPath: List[String]): FlowType = {
    val signaller = new KinesisSignaller(getKinesisProducer(kinesisSignalling.amazonRegion), kinesisSignalling.stream)

    Flow[EventBatch].map { batch =>
      val valuesToSignal = batch.events.getOrElse(Nil).map { event =>
        val key = Json.getKey(keyPath, event.body)
        val value = kinesisSignalling.publishType match {
          case PublishType.KeysOnly =>
            key.getBytes(Encoding)

          case PublishType.EventsUncompressed =>
            Json.write(event).getBytes(Encoding)

          case PublishType.EventsCompressed =>
            Compressor.compress(Json.write(event))
        }
        (key, value)
      }
      signaller.signal(valuesToSignal)
      batch
    }
      .addAttributes(ActorAttributes.dispatcher(Config.Akka.KinesisDispatcher))
      .async
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

      case EmptyDestination =>
        throw new Exception("EmptyDestination is not supported")
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

object PipelineBuilder {
  private class EventBatchCountLogger(id: String, partitionId: String) {
    private val logger = LoggerFactory.getLogger(classOf[EventBatchCountLogger])

    private var counter = 0

    def log(batch: EventBatch): Unit = {
      counter += 1
      if (counter % 200 == 0) {
        logger.debug(s"Pipeline $id in partition $partitionId processed $counter batches")
      }
    }
  }
}