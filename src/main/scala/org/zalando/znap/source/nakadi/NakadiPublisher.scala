package org.zalando.znap.source.nakadi

import java.time.ZonedDateTime
import java.util.concurrent.TimeoutException

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.coding.Deflate
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers._
import akka.stream.scaladsl.{Framing, Source}
import akka.stream.{ActorMaterializerSettings, _}
import akka.util.ByteString
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import org.slf4j.LoggerFactory
import org.zalando.znap.PartitionId
import org.zalando.znap.config.{Config, NakadiSource}
import org.zalando.znap.objects.Partition
import org.zalando.znap.persistence.OffsetReaderSync
import org.zalando.znap.source.nakadi.objects.EventBatch
import org.zalando.znap.utils._

import scala.concurrent.{Await, Future}
import scala.util.control.NonFatal

class NakadiPublisher(nakadiSource: NakadiSource,
                      tokens: NakadiTokens,
                      offsetReader: OffsetReaderSync)
                     (implicit actorSystem: ActorSystem) {
  import NakadiPublisher._

  private val logger = LoggerFactory.getLogger(classOf[NakadiPublisher])
  private implicit val materializer = ActorMaterializer(ActorMaterializerSettings(actorSystem))
  private val http = Http(actorSystem)
  private implicit val executionContext = actorSystem.dispatcher
  private val partitionGetter = new NakadiPartitionGetter(nakadiSource, tokens)

  /**
    * Create a stream source for each partition.
    */
  def createSources(): List[(PartitionId, Source[EventBatch, NotUsed])] = {
    // Get existing partitions from Nakadi.
    val partitionsListF = partitionGetter.getPartitions
    // Get saved offsets.
    val offsetsMapF = offsetReader.getLastOffsets

    // This code is called only once on the start,
    // and staying with Future would complicate it badly,
    // so we'll just block and wait for the results.
    import scala.concurrent.duration._
    val waitDuration = 15.seconds
    val partitionsList = try {
      Await.result(partitionsListF, waitDuration)
    } catch {
      case e: TimeoutException =>
        logger.error(s"Couldn't get partitions in $waitDuration.")
        throw e
    }

    logger.info(s"Got partitions for $nakadiSource: $partitionsList")

    val offsetsMap = Await.result(offsetsMapF, waitDuration)

    // Calculate offsets for each partition to start consuming from.
    val offsetsToStart = partitionsList.map { p =>
      val offset = offsetsMap.get(p.partition) match {
        case Some(storedOffset) =>
          // Check offset availability considering the stored offset.
          checkStoredOffset(storedOffset, p)
          storedOffset

        case _ => "BEGIN"
      }
      p.partition -> offset
    }

    offsetsToStart.map {
      case (partition, offset) =>
        (partition, createSourceForPartition(partition, offset))
    }
  }

  /**
    * Check if it's possible to start consuming from the stored offset,
    * given the partition with its available offsets.
    * @param partition partition to be consumed from `storedOffset`
    */
  private def checkStoredOffset(storedOffset: String, partition: Partition): Unit = {
    val storedOffsetL = storedOffset.toLong
    val oldestAvailableOffsetL = partition.oldestAvailableOffset.toLong
    val newestAvailableOffsetL = partition.newestAvailableOffset.toLong
    if (storedOffsetL < oldestAvailableOffsetL || storedOffsetL > newestAvailableOffsetL) {
      val message = "Available offsets: " +
        s"${partition.oldestAvailableOffset}-${partition.newestAvailableOffset}, " +
        s"stored offset: $storedOffset"
      logger.error(message)
      throw new InvalidOffsetException(partition.partition, storedOffset, message)
    }
  }

  private def createSourceForPartition(partition: String,
                                       offset: String): Source[EventBatch, NotUsed] = {
    val recoveryStrategy =
      // knownOffset is None to indicate that it should be read from a store.
      new StreamRecoveryStrategy(() => createSourceForPartition0(partition, None))

    createSourceForPartition0(partition, Some(offset))
      .recoverWithRetries(-1, recoveryStrategy)
    .async
  }

  /**
    * Create a source for the partition.
    *
    * If `knownOffset` is Some(_), it's used as a start offset;
    * otherwise the last offset would be received (if exists) and used.
    * @param partition
    * @param knownOffset
    * @return
    */
  private def createSourceForPartition0(partition: String,
                                        knownOffset: Option[String]): Source[EventBatch, NotUsed] = {
    logger.debug(s"Creating Source for $nakadiSource, partition $partition. knownOffset used is $knownOffset")

    val offsetToUseF = knownOffset match {
      case Some(offset) =>
        Future.successful(offset)

      case None =>
        offsetReader.getLastOffsets.map(lastOffsets => lastOffsets.getOrElse(partition, "BEGIN"))
    }

    Source.fromFuture(offsetToUseF).flatMapConcat { offset =>
      createPartitionResponseSource(partition, offset)
        .flatMapConcat {
          case response @ HttpResponse(StatusCodes.OK, _, _, _) =>
            processNormalResponse(response)

          // If partitions are cleared on the backend, the local offset might become further.
          // To prevent this, we throw the special exception.
          case response @ HttpResponse(StatusCodes.PreconditionFailed, _, _, _) =>
            processPreconditionFailedResponse(partition, offset, response)

          case unknownResponse: HttpResponse =>
            processUnknownResponse(partition, unknownResponse)
        }

    }
  }

  private def createPartitionResponseSource(partition: String, offset: String): Source[HttpResponse, NotUsed] = {
    val uri = s"/event-types/${nakadiSource.eventType}/events" + "?stream_timeout=0" +
      nakadiSource.batchLimit.map(bl => s"&batch_limit=$bl").getOrElse("")

    val authorizationHeader = new Authorization(OAuth2BearerToken(tokens.get()))
    val xNakadiCursor = new XNakadiCursors(partition, offset)
    val acceptEncoding = `Accept-Encoding`(HttpEncodings.deflate)
    var headers = List(authorizationHeader, xNakadiCursor)
    if (nakadiSource.compress) {
      headers = headers :+ acceptEncoding
    }

    val nakadiConnectionFlow =
      if (nakadiSource.uri.getScheme.equals("https")) {
        http.outgoingConnectionHttps(nakadiSource.uri.getHost, nakadiSource.uri.getPort)
      } else {
        http.outgoingConnection(nakadiSource.uri.getHost, nakadiSource.uri.getPort)
      }

    Source.single(HttpRequest(HttpMethods.GET, uri, headers))
      .via(nakadiConnectionFlow)
  }

  private def processNormalResponse(response: HttpResponse): Source[EventBatch, Any] = {
    val result = response.entity.withSizeLimit(Config.HttpStreamingMaxSize)
      .dataBytes

    val decodedResult = decodeIfNeeded(result, response.header[`Content-Encoding`])

    decodedResult
      // Coalesce chunks into a line.
      .via(Framing.delimiter(ByteString("\n"), Int.MaxValue))

      .map(bs => Json.read[EventBatch](bs.utf8String))
  }

  private def processPreconditionFailedResponse(partition: String, offset: String, response: HttpResponse): Source[Nothing, Any] = {
    val offsetUnavailableRegex = s"""^offset\\s\\d+\\sfor\\spartition\\s$partition\\sis\\sunavailable$$""".r
    val result = response.entity.dataBytes

    val decodedResult = decodeIfNeeded(result, response.header[`Content-Encoding`])

    decodedResult.fold("")(_ + " " + _.utf8String).map { content =>
      val preconditionFailedContent = Json.read[PreconditionFailedContent](content)
      assert(preconditionFailedContent.status == StatusCodes.PreconditionFailed.intValue)

      if (offsetUnavailableRegex.findFirstMatchIn(preconditionFailedContent.detail).isDefined) {
        throw new InvalidOffsetException(partition, offset, content)
      } else {
        val msg = s"Unknown response on getting events from partition $partition: $response $content"
        throw new Exception(msg)
      }
    }
  }

  private def processUnknownResponse(partition: String, unknownResponse: HttpResponse): Source[Nothing, Any] = {
    val result = unknownResponse.entity.dataBytes

    val decodedResult = decodeIfNeeded(result, unknownResponse.header[`Content-Encoding`])

    decodedResult.map { bs =>
      val msg = s"Unknown response on getting events from partition $partition: $unknownResponse ${bs.utf8String}"
      throw new Exception(msg)
    }
  }
}

object NakadiPublisher {
  final class StreamCompletedException extends Exception


  @JsonIgnoreProperties(ignoreUnknown = true)
  final case class PreconditionFailedContent(`type`: String,
                                             title: String,
                                             status: Int,
                                             detail: String)

  private class StreamRecoveryStrategy(getStream: () => Source[EventBatch, NotUsed])
      extends PartialFunction[Throwable, Graph[SourceShape[EventBatch], NotUsed]] {
    private val logger = LoggerFactory.getLogger(classOf[StreamRecoveryStrategy])

    private val errorTracker = new TimePeriodEventTracker(
      Config.Supervision.NakadiReader.MaxFailures,
      Config.Supervision.NakadiReader.Period
    )

    /**
      * @return `true`, if the stream should be restarted; otherwise false.
      */
    override def isDefinedAt(x: Throwable): Boolean = {
      x match {
        // Just restart in case of StreamCompleted.
        case _: StreamCompletedException =>
          logger.debug("Nakadi stream failed with StreamCompletedException exception, restarting")
          true

        // Always fail in case of InvalidOffsetException.
        case ex: InvalidOffsetException =>
          logger.debug(s"Nakadi stream failed with InvalidOffsetException, stopping: ${ex.message}")
          false

        case NonFatal(ex) =>
          val tooManyErrors = errorTracker.registerEvent(ZonedDateTime.now())
          if (tooManyErrors) {
            logger.error(s"Exception from Nakadi stream. " +
              s"Too many exceptions in last ${errorTracker.period}: ${ThrowableUtils.getStackTraceString(ex)}")
            false
          } else {
            logger.warn(s"Exception from Nakadi stream, " +
              s"will be restarted: ${ThrowableUtils.getStackTraceString(ex)}")
            true
          }
      }
    }

    override def apply(v1: Throwable): Graph[SourceShape[EventBatch], NotUsed] = {
      getStream()
    }
  }


  private def decodeIfNeeded(dataBytes: Source[ByteString, Any],
                             contentEncodingHeader: Option[`Content-Encoding`]): Source[ByteString, Any] = {
    contentEncodingHeader match {
      case Some(`Content-Encoding`(encodings)) if encodings.contains(HttpEncodings.deflate) =>
        dataBytes.via(Deflate.decoderFlow)

      case _ =>
        dataBytes
    }
  }
}
