package org.zalando.znap.source.nakadi

import java.time.ZonedDateTime

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.stream.scaladsl.{Merge, Source}
import akka.stream.{ActorMaterializerSettings, _}
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import org.slf4j.LoggerFactory
import org.zalando.znap.config.{Config, NakadiSource}
import org.zalando.znap.source.nakadi.objects.{EventBatch, NakadiPartition}
import org.zalando.znap.objects.Partition
import org.zalando.znap.persistence.OffsetReaderSync
import org.zalando.znap.utils._

import scala.concurrent.Future
import scala.util.control.NonFatal

class NakadiPublisher(nakadiSource: NakadiSource,
                      tokens: NakadiTokens,
                      offsetReader: OffsetReaderSync)
                     (implicit actorSystem: ActorSystem) {
  import NakadiPublisher._
  import org.zalando.znap.utils.RichStream._

  private val logger = LoggerFactory.getLogger(classOf[NakadiPublisher])
  private implicit val materializer = ActorMaterializer(ActorMaterializerSettings(actorSystem))
  private val http = Http(actorSystem)
  private implicit val executionContext = actorSystem.dispatcher

  def getSource(): Source[EventBatch, NotUsed] = {
    // Get existing partitions from Nakadi.
    val partitionsF = getPartitions.flatMap {
      case HttpResponse(StatusCodes.OK, _, entity, _) =>
        entity.dataBytes.collectAsObject[List[NakadiPartition]]().map { nakadiPartitions =>
          val partitions = nakadiPartitions.map { np =>
            Partition(np.partition, np.oldestAvailableOffset, np.newestAvailableOffset)
          }
          partitions
        }

      case unknownResponse: HttpResponse =>
        unknownResponse.entity.dataBytes.collectAsString().map { content =>
          throw new Exception(s"Error response on getting partitions, ${unknownResponse.status} $content")
        }
    }

    // Get saved offsets.
    val offsetsF = offsetReader.getLastOffsets

    val offsetsToStartF = (partitionsF zip offsetsF).map {
      case (partitionsList, offsetsMap) =>
        partitionsList.map {
          case Partition(partition, oldestAvailableOffset, newestAvailableOffset) =>
            val offset = offsetsMap.get(partition) match {
              case Some(storedOffset) =>
                // Check offset availability considering the stored offset.
                val storedOffsetL = storedOffset.toLong
                val oldestAvailableOffsetL = oldestAvailableOffset.toLong
                val newestAvailableOffsetL = newestAvailableOffset.toLong
                if (storedOffsetL < oldestAvailableOffsetL || storedOffsetL > newestAvailableOffsetL) {
                  val message = s"Available offsets: $oldestAvailableOffset-$newestAvailableOffset, " +
                    s"stored offset: $storedOffset"
                  logger.error(message)
                  throw new InvalidOffsetException(partition, storedOffset, message)
                }

                storedOffset

              case _ => "BEGIN"
            }
            partition -> offset
        }
    }

    Source.fromFuture(offsetsToStartF).flatMapConcat { offsetsToStart =>
      val partitionStreams = offsetsToStart.map {
        case (partition, offset) =>
          getSourceForPartition(partition, offset)
      }

      if (partitionStreams.size == 1) {
        partitionStreams.head
      } else {
        val s1 = partitionStreams.head
        val s2 = partitionStreams(1)
        val rest = partitionStreams.drop(2)
        Source.combine(s1, s2, rest:_*)(Merge(_))
      }
    }
  }

  private def getSourceForPartition(partition: String,
                                    offset: String): Source[EventBatch, NotUsed] = {
    val recoveryStrategy =
      new StreamRecoveryStrategy(() => getSourceForPartition0(partition, None))

    getSourceForPartition0(partition, Some(offset))
      .recoverWithRetries(-1, recoveryStrategy)
    .async
  }

  private def getSourceForPartition0(partition: String,
                                     knownOffset: Option[String]): Source[EventBatch, NotUsed] = {
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
            processUnkownResponse(partition, unknownResponse)
        }

    }
  }

  private def getPartitions: Future[HttpResponse] = {
    // Request partitions of the topic.
    val scheme = nakadiSource.uri.getScheme
    val hostAndPort = nakadiSource.uri.getAuthority
    val uri = s"$scheme://$hostAndPort/event-types/${nakadiSource.eventType}/partitions"
    val authorizationHeader = new Authorization(OAuth2BearerToken(tokens.get()))
    val request = HttpRequest(
      uri = uri,
      headers = List(authorizationHeader))

    http.singleRequest(request)
  }

  private def createPartitionResponseSource(partition: String, offset: String): Source[HttpResponse, NotUsed] = {
    val uri = s"/event-types/${nakadiSource.eventType}/events" + "?stream_timeout=0"

    val authorizationHeader = new Authorization(OAuth2BearerToken(tokens.get()))
    val xNakadiCursor = new XNakadiCursors(partition, offset)
    val headers = List(authorizationHeader, xNakadiCursor)

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
    response.entity.withSizeLimit(Config.HttpStreamingMaxSize)
      .dataBytes
      // Coalesce chunks into a line.
      .scan("") { (acc, chunk) =>
      if (acc.contains("\n")) {
        chunk.utf8String
      } else {
        acc + chunk.utf8String
      }
    }
    .filter(_.contains("\n"))

    .map(Json.read[EventBatch])
  }

  private def processPreconditionFailedResponse(partition: String, offset: String, response: HttpResponse): Source[Nothing, Any] = {
    val offsetUnavailableRegex = s"""^offset\\s\\d+\\sfor\\spartition\\s$partition\\sis\\sunavailable$$""".r
    response.entity.dataBytes.fold("")(_ + " " + _.utf8String).map { content =>
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

  private def processUnkownResponse(partition: String, unknownResponse: HttpResponse): Source[Nothing, Any] = {
    unknownResponse.entity.dataBytes.map { bs =>
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
}