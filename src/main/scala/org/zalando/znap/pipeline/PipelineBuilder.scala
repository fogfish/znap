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
import nl.grons.metrics.scala.Meter
import org.slf4j.LoggerFactory
import org.zalando.znap.{PartitionId, TargetId}
import org.zalando.znap.config._
import org.zalando.znap.metrics.Instrumented
import org.zalando.znap.persistence.OffsetReaderSync
import org.zalando.znap.persistence.disk.{DiskEventsWriter, DiskOffsetReader, DiskOffsetWriter}
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

  private var batchProcessingFinishedMeters = Map.empty[String, Meter]
  private def getBatchProcessingFinishedMeter(targetId: TargetId): Meter = {
    batchProcessingFinishedMeters.get(targetId) match {
      case Some(meter) =>
        meter
      case _ =>
        val meter = metrics.meter(s"batches-processed-$targetId")
        batchProcessingFinishedMeters += targetId -> meter
        meter
    }
  }
  private var eventProcessingFinishedMeters = Map.empty[String, Meter]
  private def getEventProcessingFinishedMeter(targetId: TargetId): Meter = {
    eventProcessingFinishedMeters.get(targetId) match {
      case Some(meter) =>
        meter
      case _ =>
        val meter = metrics.meter(s"events-processed-$targetId")
        eventProcessingFinishedMeters += targetId -> meter
        meter
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
      val metricsStep = buildMetricsStep(snapshotTarget)

//      val sink = Sink.ignore
//      val sink = Sink.foreach(println)

      val eventBatchCountLogger = new EventBatchCountLogger(id, partitionId)
      val sink = Sink.foreach[(EventBatch, ProcessingContext)] {
        case (batch, processingContext) =>
          eventBatchCountLogger.log(batch, processingContext)
      }

      val graph = source
        .map(b => (b, ProcessingContext()))
        .via(filterByEventClassStep)
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

  private def buildOffsetReader(snapshotTarget: SnapshotTarget): OffsetReaderSync = {
    val offsetReader = snapshotTarget.offsetPersistence match {
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

      case EmptySource =>
        throw new Exception("EmptySource is not supported")
    }
  }

  private def buildFilterByEventClassStep(source: SnapshotSource): FlowType = {
    source match {
      case nakadiSource: NakadiSource =>
        Flow[(EventBatch, ProcessingContext)].map { case (eventBatch, processingContext) =>
          val timeStart = getTime
          val filteredEventBatch = nakadiSource.filter match {
            case Some(SourceFilter(filterField, filterValues)) =>
              val filteredEvents = eventBatch.events.map { eventList =>
                eventList.filter(e => filterValues.contains(e.get(filterField).asText()))
              }
              EventBatch(eventBatch.cursor, filteredEvents)

            case _ =>
              eventBatch
          }
          val timeFinish = getTime

          (filteredEventBatch,
            processingContext.saveStageTime("filter", timeStart, timeFinish))
        }.async

      case EmptySource =>
        throw new Exception("EmptySource is not supported")
    }
  }

  private def buildDataWriteStep(snapshotTarget: SnapshotTarget): FlowType = {
    val (eventsWriter, dispatcherAttributes, writerName) = snapshotTarget.destination match {
      case dynamoDBDestination: DynamoDBDestination =>
        val uri = dynamoDBDestination.uri.toString
        (new DynamoDBEventsWriter(snapshotTarget, getDynamoDB(uri)),
          ActorAttributes.dispatcher(Config.Akka.DynamoDBDispatcher),
          "dynamo")

      case _: DiskDestination =>
        (new DiskEventsWriter(snapshotTarget),
          ActorAttributes.dispatcher(Config.Akka.DiskDBDispatcher),
          "disk")

      case EmptyDestination =>
        throw new Exception("EmptyDestination is not supported")
    }

    eventsWriter.init()

    Flow[(EventBatch, ProcessingContext)].map { case (batch, processingContext) =>
      val timeStart = getTime
      eventsWriter.write(batch.events.getOrElse(Nil))
      val timeFinish = getTime
      (batch, processingContext.saveStageTime(s"write-$writerName", timeStart, timeFinish))
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
        Flow[(EventBatch, ProcessingContext)].map(x => x) // empty
    }
  }

  private def buildSqsSignalStep(sqsSignalling: SqsSignalling, keyPath: List[String]): FlowType = {
    val signaller = new SqsSignaller(sqsSignalling, sqsClient)

    Flow[(EventBatch, ProcessingContext)].map { case (batch, processingContext) =>
      val timeStart = getTime
      val valuesToSignal = batch.events.getOrElse(Nil).map { event =>
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

  private def buildKinesisSignalStep(kinesisSignalling: KinesisSignalling, keyPath: List[String]): FlowType = {
    val signaller = new KinesisSignaller(getKinesisProducer(kinesisSignalling.amazonRegion), kinesisSignalling.stream)

    Flow[(EventBatch, ProcessingContext)].map { case (batch, processingContext) =>
      val timeStart = getTime
      val valuesToSignal = batch.events.getOrElse(Nil).map { event =>
        val key = Json.getKey(keyPath, event)
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
      val timeFinish = getTime
      (batch, processingContext.saveStageTime("signal-kinesis", timeStart, timeFinish))
    }
      .addAttributes(ActorAttributes.dispatcher(Config.Akka.KinesisDispatcher))
      .async
  }

  private def buildOffsetWriteStep(snapshotTarget: SnapshotTarget): FlowType = {
    val (props, dispatcherAttributes, writerName) = snapshotTarget.offsetPersistence match {
      case dynamoDBOffsetPersistence: DynamoDBOffsetPersistence =>
        val uri = dynamoDBOffsetPersistence.uri.toString
        val offsetWriter = new DynamoDBOffsetWriter(dynamoDBOffsetPersistence, getDynamoDB(uri))
        offsetWriter.init()

        val props = OffsetWriterActor.props(
          snapshotTarget, Config.Persistence.DynamoDB.OffsetWritePeriod, offsetWriter)
          .withDispatcher(Config.Akka.DynamoDBDispatcher)

        (props,
          ActorAttributes.dispatcher(Config.Akka.DynamoDBDispatcher),
          "dynamo")

//      case _: DiskDestination =>
//        val offsetWriter = new DiskOffsetWriter(snapshotTarget)
//        offsetWriter.init()
//
//        val props = OffsetWriterActor.props(
//          snapshotTarget, Config.Persistence.Disk.OffsetWritePeriod, offsetWriter)
//          .withDispatcher(Config.Akka.DiskDBDispatcher)
//
//        (props,
//          ActorAttributes.dispatcher(Config.Akka.DiskDBDispatcher),
//          "disk")

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

  private def buildMetricsStep(snapshotTarget: SnapshotTarget): FlowType = {
    val batchProcessingFinishedMeter = getBatchProcessingFinishedMeter(snapshotTarget.id)
    val eventProcessingFinishedMeter = getEventProcessingFinishedMeter(snapshotTarget.id)
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