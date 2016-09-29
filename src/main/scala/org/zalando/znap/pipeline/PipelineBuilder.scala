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
import nl.grons.metrics.scala.Meter
import org.slf4j.LoggerFactory
import org.zalando.znap.{PartitionId, PipelineId}
import org.zalando.znap.config._
import org.zalando.znap.metrics.Instrumented
import org.zalando.znap.persistence.OffsetReaderSync
import org.zalando.znap.persistence.dynamo.{DynamoDBEventsWriter, DynamoDBOffsetReader, DynamoDBOffsetWriter}
import org.zalando.znap.signalling.kinesis.KinesisSignaller
import org.zalando.znap.signalling.sqs.SqsSignaller
import org.zalando.znap.source.nakadi.objects.EventBatch
import org.zalando.znap.source.nakadi.{NakadiPublisher, NakadiTokens}
import org.zalando.znap.utils.{Compressor, Json}

import scala.util.control.NonFatal

private class PipelineBuilder(tokens: NakadiTokens)(actorSystem: ActorSystem) extends Instrumented {

  import PipelineBuilder._

  private type FlowType = Flow[(EventBatch, ProcessingContext), (EventBatch, ProcessingContext), NotUsed]

  private val Encoding = "UTF-8"

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

  private var batchProcessingFinishedMeters = Map.empty[String, Meter]
  private def getBatchProcessingFinishedMeter(pipelineId: PipelineId): Meter = {
    batchProcessingFinishedMeters.get(pipelineId) match {
      case Some(meter) =>
        meter
      case _ =>
        val meter = metrics.meter(s"batches-processed-$pipelineId")
        batchProcessingFinishedMeters += pipelineId -> meter
        meter
    }
  }
  private var eventProcessingFinishedMeters = Map.empty[String, Meter]
  private def getEventProcessingFinishedMeter(pipelineId: PipelineId): Meter = {
    eventProcessingFinishedMeters.get(pipelineId) match {
      case Some(meter) =>
        meter
      case _ =>
        val meter = metrics.meter(s"events-processed-$pipelineId")
        eventProcessingFinishedMeters += pipelineId -> meter
        meter
    }
  }

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
      val metricsStep = buildMetricsStep(snapshotPipeline)

//      val sink = Sink.ignore
//      val sink = Sink.foreach(println)

      val eventBatchCountLogger = new EventBatchCountLogger(id, partitionId)
      val sink = Sink.foreach[(EventBatch, ProcessingContext)] {
        case (batch, processingContext) =>
          eventBatchCountLogger.log(batch, processingContext)
      }

      val graph = source
        .map(b => (b, ProcessingContext()))
        .via(dataWriteStep)
        .via(signalStep)
        .via(offsetWriteStep)
        .via(metricsStep)
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
        new DynamoDBOffsetReader(dynamoDBOffsetPersistence, getDynamoDB(uri))(dynamoExecutionContext)

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
          (new DynamoDBEventsWriter(target, getDynamoDB(uri)),
            ActorAttributes.dispatcher(Config.Akka.DynamoDBDispatcher),
            "dynamo")

        case EmptyDestination =>
          throw new Exception("EmptyDestination is not supported")
      }
      eventsWriter.init()

      Flow[(EventBatch, ProcessingContext)].map { case (batch, processingContext) =>
      val filteredEvents = filterEvents(target, batch)

      val timeStart = getTime
        eventsWriter.write(filteredEvents)
        val timeFinish = getTime
        (batch, processingContext.saveStageTime(s"write-$writerName", timeStart, timeFinish))
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

//        case Some(kinesisSignalling: KinesisSignalling) =>
//          buildKinesisSignalStep(kinesisSignalling, target.key)

        case None =>
          Flow[(EventBatch, ProcessingContext)].map(x => x) // empty
      }
    }

    flows.reduceLeft[FlowType] {
      case (x, y) => x.via(y)
    }
  }

  private def buildSqsSignalStep(target: SnapshotTarget, sqsSignalling: SqsSignalling, keyPath: List[String]): FlowType = {
    val signaller = new SqsSignaller(sqsSignalling, sqsClient)

    Flow[(EventBatch, ProcessingContext)].map { case (batch, processingContext) =>
      val filteredEvents = filterEvents(target, batch)

      val timeStart = getTime
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
      signaller.signal(valuesToSignal)
      val timeFinish = getTime
      (batch, processingContext.saveStageTime("signal-sqs", timeStart, timeFinish))
    }
      .addAttributes(ActorAttributes.dispatcher(Config.Akka.SqsDispatcher))
      .async
  }

//  private def buildKinesisSignalStep(kinesisSignalling: KinesisSignalling, keyPath: List[String]): FlowType = {
//    val signaller = new KinesisSignaller(getKinesisProducer(kinesisSignalling.amazonRegion), kinesisSignalling.stream)
//
//    Flow[(EventBatch, ProcessingContext)].map { case (batch, processingContext) =>
//      val timeStart = getTime
//      val valuesToSignal = batch.events.getOrElse(Nil).map { event =>
//        val key = Json.getKey(keyPath, event)
//        val value = kinesisSignalling.publishType match {
//          case PublishType.KeysOnly =>
//            key.getBytes(Encoding)
//
//          case PublishType.EventsUncompressed =>
//            Json.write(event).getBytes(Encoding)
//
//          case PublishType.EventsCompressed =>
//            Compressor.compress(Json.write(event))
//        }
//        (key, value)
//      }
//      signaller.signal(valuesToSignal)
//      val timeFinish = getTime
//      (batch, processingContext.saveStageTime("signal-kinesis", timeStart, timeFinish))
//    }
//      .addAttributes(ActorAttributes.dispatcher(Config.Akka.KinesisDispatcher))
//      .async
//  }

  private def buildOffsetWriteStep(snapshotPipeline: SnapshotPipeline): FlowType = {
    val (props, dispatcherAttributes, writerName) = snapshotPipeline.offsetPersistence match {
      case dynamoDBOffsetPersistence: DynamoDBOffsetPersistence =>
        val uri = dynamoDBOffsetPersistence.uri.toString
        val offsetWriter = new DynamoDBOffsetWriter(dynamoDBOffsetPersistence, getDynamoDB(uri))
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

    val branch = Flow[(EventBatch, ProcessingContext)]
      .map { case (eventBatch, _) => eventBatch.cursor }
      .to(Sink.actorSubscriber(props))
      .addAttributes(dispatcherAttributes)
      .async

    Flow[(EventBatch, ProcessingContext)]
      .alsoTo(branch)
  }

  private def buildMetricsStep(snapshotPipeline: SnapshotPipeline): FlowType = {
    val batchProcessingFinishedMeter = getBatchProcessingFinishedMeter(snapshotPipeline.id)
    val eventProcessingFinishedMeter = getEventProcessingFinishedMeter(snapshotPipeline.id)
    Flow[(EventBatch, ProcessingContext)].map { case r @ (batch, _) =>
      batchProcessingFinishedMeter.mark()
      eventProcessingFinishedMeter.mark(batch.events.size)
      r
    }
  }
}

object PipelineBuilder {
  private class EventBatchCountLogger(id: String, partitionId: String) {
    private val logger = LoggerFactory.getLogger(classOf[EventBatchCountLogger])

    private var counter = 0

    private var stageDurationSums = Map.empty[String, Long]
    private var previousWriteDynamoFinished: Option[Long] = None

    def log(batch: EventBatch, processingContext: ProcessingContext): Unit = {
      processingContext.stageTimes.foreach {
        case (stage, start, finish) if stage == "write-dynamo" || stage == "signal-sqs" =>
          val duration = finish - start
          val sum = stageDurationSums.getOrElse(stage, 0L) + duration
          stageDurationSums += stage -> sum


          if (previousWriteDynamoFinished.nonEmpty && stage == "write-dynamo") {
            val betweenStage = "between-two-write-dynamo"
            val duration = start - previousWriteDynamoFinished.get
            val sum = stageDurationSums.getOrElse(betweenStage, 0L) + duration
            stageDurationSums += betweenStage -> sum
          }
          previousWriteDynamoFinished = Some(finish)

        case _ =>
      }

      val statisticsWindow = 200
      counter += 1
      if (counter % statisticsWindow == 0) {
        val averagePerStage = stageDurationSums.map { case (stage, sum) =>
          s"$stage - ${sum / statisticsWindow}"
        }.mkString(", ")
        stageDurationSums = Map.empty

        logger.debug(s"Pipeline $id in partition $partitionId processed $counter batches. Avg. durations: $averagePerStage")
      }
    }
  }

  private case class ProcessingContext(stageTimes: List[(String, Long, Long)]) {
    def saveStageTime(stage: String, start: Long, finish: Long): ProcessingContext = {
      this.copy(stageTimes = (stage, start, finish) :: stageTimes)
    }
  }

  private object ProcessingContext {
    def apply(): ProcessingContext = ProcessingContext(Nil)
  }

  private def getTime: Long = {
    System.currentTimeMillis()
  }
}