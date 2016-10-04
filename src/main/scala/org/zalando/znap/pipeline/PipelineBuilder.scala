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
import com.fasterxml.jackson.databind.JsonNode
import org.zalando.znap.{PartitionId, PipelineId}
import org.zalando.znap.config._
import org.zalando.znap.metrics.Instrumented
import org.zalando.znap.persistence.OffsetReaderSync
import org.zalando.znap.persistence.dynamo.{DynamoDBEventsWriter, DynamoDBOffsetReader, DynamoDBOffsetWriter}
import org.zalando.znap.signalling.sqs.SqsSignaller
import org.zalando.znap.source.nakadi.objects.EventBatch
import org.zalando.znap.source.nakadi.{NakadiPublisher, NakadiTokens}
import org.zalando.znap.utils.{Compressor, Json}

import scala.util.control.NonFatal

private class PipelineBuilder(tokens: NakadiTokens)(actorSystem: ActorSystem) extends Instrumented {

  private type FlowType = Flow[EventBatch, EventBatch, NotUsed]

  private val Encoding = "UTF-8"

  private lazy val dynamoExecutionContext = actorSystem.dispatchers.lookup(Config.Akka.DynamoDBDispatcher)
//  private lazy val sqsExecutionContext = actorSystem.dispatchers.lookup(Config.Akka.SqsDispatcher)

  private lazy val sqsClient = new AmazonSQSClient()

  private val dynamoDBs = new Cached({
    uri: String =>
      val dynamoDBClient = new AmazonDynamoDBClient()
      dynamoDBClient.withEndpoint(uri)
      new DynamoDB(dynamoDBClient)
  })

//  private val kinesisProducers = new Cached({
//    amazonRegion: String =>
//      val config = new KinesisProducerConfiguration
//      config.setRegion(amazonRegion)
//      new KinesisProducer(config)
//  })


  private val batchProcessingFinishedMeters = new Cached({
    pipelineId: PipelineId => metrics.meter(s"batches-processed-$pipelineId")
  })

  private val eventProcessingFinishedMeters = new Cached({
    pipelineId: PipelineId => metrics.meter(s"events-processed-$pipelineId")
  })

  private val batchesInFlightCounters = new Cached({
    pipelineId: PipelineId => metrics.counter(s"batches-in-flight-$pipelineId")
  })

  private val eventsInFlightCounters = new Cached({
    pipelineId: PipelineId => metrics.counter(s"events-in-flight-$pipelineId")
  })

  private val dynamoWriteTimer = metrics.timer("dynamo-write")
  private val sqsSignalTimer = metrics.timer("sqs-signal")

  /**
    * Build a pipeline in a form of Akka Streams graph.
    * @param id the pipeline ID.
    * @param snapshotPipeline snapshot pipeline configuration.
    */
  def build(id: String, snapshotPipeline: SnapshotPipeline): List[(String, RunnablePipeline)] = {
    val offsetReader = buildOffsetReader(snapshotPipeline)
    val sources = buildSource(snapshotPipeline.source, offsetReader)

    sources.map { case (partitionId, source) =>
      val pipelineUniqueId = UUID.randomUUID().toString

//      val filterByEventClassStep = buildFilterByEventClassStep(snapshotPipeline.source)
      val dataWriteStep = buildDataWriteStep(snapshotPipeline)
      val signalStep = buildSignalStep(snapshotPipeline)
      val offsetWriteStep = buildOffsetWriteStep(snapshotPipeline)
      val metricsStartStep = buildMetricsStartStep(snapshotPipeline)
      val metricsFinishStep = buildMetricsFinishStep(snapshotPipeline)

      val sink = Sink.ignore
//      val sink = Sink.foreach(println)

//      val eventBatchCountLogger = new EventBatchCountLogger(id, partitionId)
//      val sink = Sink.foreach[(EventBatch, ProcessingContext)] {
//        case (batch, processingContext) =>
//          eventBatchCountLogger.log(batch, processingContext)
//      }

      val graph = source
        .via(metricsStartStep)
        .via(dataWriteStep)
        .via(signalStep)
        .via(offsetWriteStep)
        .via(metricsFinishStep)
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

  private def buildOffsetReader(snapshotPipeline: SnapshotPipeline): OffsetReaderSync = {
    val offsetReader = snapshotPipeline.offsetPersistence match {
      case dynamoDBOffsetPersistence: DynamoDBOffsetPersistence =>
        val uri = dynamoDBOffsetPersistence.uri.toString
        new DynamoDBOffsetReader(dynamoDBOffsetPersistence, dynamoDBs(uri))(dynamoExecutionContext)

      case EmptyOffsetPersistence  =>
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
    }
  }

  private def buildDataWriteStep(snapshotPipeline: SnapshotPipeline): FlowType = {
    val flows = snapshotPipeline.targets.map { target =>
      val (eventsWriter, dispatcherAttributes, writerName) = target.destination match {
        case dynamoDBDestination: DynamoDBDestination =>
          val uri = dynamoDBDestination.uri.toString
          (new DynamoDBEventsWriter(target, dynamoDBs(uri)),
            ActorAttributes.dispatcher(Config.Akka.DynamoDBDispatcher),
            "dynamo")

        case EmptyDestination =>
          throw new Exception("EmptyDestination is not supported")
      }
      eventsWriter.init()

      Flow[EventBatch].map { batch =>
        val filteredEvents = filterEvents(target, batch)

        dynamoWriteTimer.time {
          eventsWriter.write(filteredEvents)
        }

        batch
      }
        .addAttributes(dispatcherAttributes)
        .async
    }

    flows.reduceLeft[FlowType] {
      case (x, y) => x.via(y)
    }
  }

  def filterEvents(target: SnapshotTarget, batch: EventBatch): List[JsonNode] = {
    val filteredEvents = target.filter match {
      case Some(SourceFilter(filterField, filterValues)) =>
        val filteredEvents = batch.events.map { eventList =>
          eventList.filter(e => filterValues.contains(e.get(filterField).asText()))
        }
        filteredEvents.getOrElse(Nil)

      case _ =>
        batch.events.getOrElse(Nil)
    }
    filteredEvents
  }

  private def buildSignalStep(snapshotPipeline: SnapshotPipeline): FlowType = {
    val flows = snapshotPipeline.targets.map { target =>
      target.signalling match {
        case Some(sqsSignalling: SqsSignalling) =>
          buildSqsSignalStep(target, sqsSignalling, target.key)

        case Some(kinesisSignalling: KinesisSignalling) =>
          ???

        case None =>
          Flow[EventBatch].map(x => x) // empty
      }
    }

    flows.reduceLeft[FlowType] {
      case (x, y) => x.via(y)
    }
  }

  private def buildSqsSignalStep(target: SnapshotTarget, sqsSignalling: SqsSignalling, keyPath: List[String]): FlowType = {
    val signaller = new SqsSignaller(sqsSignalling, sqsClient)

    Flow[EventBatch].map { batch =>
      val filteredEvents = filterEvents(target, batch)

      val valuesToSignal = filteredEvents.map { event =>
        sqsSignalling.publishType match {
          case PublishType.KeysOnly =>
            Json.getKey(keyPath, event)

          case PublishType.EventsUncompressed =>
            Json.write(event)

          case PublishType.EventsCompressed =>
            Compressor.compressBase64(Json.write(event))
        }
      }

      sqsSignalTimer.time {
        signaller.signal(valuesToSignal)
      }

      batch
    }
      .addAttributes(ActorAttributes.dispatcher(Config.Akka.SqsDispatcher))
      .async
  }

  private def buildOffsetWriteStep(snapshotPipeline: SnapshotPipeline): FlowType = {
    val (props, dispatcherAttributes, writerName) = snapshotPipeline.offsetPersistence match {
      case dynamoDBOffsetPersistence: DynamoDBOffsetPersistence =>
        val uri = dynamoDBOffsetPersistence.uri.toString
        val offsetWriter = new DynamoDBOffsetWriter(dynamoDBOffsetPersistence, dynamoDBs(uri))
        offsetWriter.init()

        val props = OffsetWriterActor.props(
          snapshotPipeline, Config.Persistence.DynamoDB.OffsetWritePeriod, offsetWriter)
          .withDispatcher(Config.Akka.DynamoDBDispatcher)

        (props,
          ActorAttributes.dispatcher(Config.Akka.DynamoDBDispatcher),
          "dynamo")

      case EmptyOffsetPersistence =>
        throw new Exception("EmptyOffsetPersistence is not supported")
    }

    val branch = Flow[EventBatch]
      .map(_.cursor)
      .to(Sink.actorSubscriber(props))
      .addAttributes(dispatcherAttributes)
      .async

    Flow[EventBatch]
      .alsoTo(branch)
  }

  private def buildMetricsStartStep(snapshotPipeline: SnapshotPipeline): FlowType = {
    val batchesInFlightCounter = batchesInFlightCounters(snapshotPipeline.id)
    val eventsInFlightCounter = eventsInFlightCounters(snapshotPipeline.id)
    Flow[EventBatch].map { batch =>
      val batchSize = batch.events.size

      batchesInFlightCounter.inc()
      eventsInFlightCounter.inc(batchSize)

      batch
    }
  }

  private def buildMetricsFinishStep(snapshotPipeline: SnapshotPipeline): FlowType = {
    val batchProcessingFinishedMeter = batchProcessingFinishedMeters(snapshotPipeline.id)
    val eventProcessingFinishedMeter = eventProcessingFinishedMeters(snapshotPipeline.id)

    val batchesInFlightCounter = batchesInFlightCounters(snapshotPipeline.id)
    val eventsInFlightCounter = eventsInFlightCounters(snapshotPipeline.id)
    Flow[EventBatch].map { batch =>
      val batchSize = batch.events.size

      batchProcessingFinishedMeter.mark()
      eventProcessingFinishedMeter.mark(batchSize)

      batchesInFlightCounter.dec()
      eventsInFlightCounter.dec(batchSize)

      batch
    }
  }


  private class Cached[K, V](create: K => V) {
    private var cache = Map.empty[K, V]
    def get(key: K): V = {
      cache.get(key) match {
        case Some(counter) =>
          counter
        case _ =>
          val value = create(key)
          cache += key -> value
          value
      }
    }

    def apply(key: K): V = {
      get(key)
    }
  }
}
