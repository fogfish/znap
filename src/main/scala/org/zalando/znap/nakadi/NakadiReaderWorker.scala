/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.nakadi

import akka.actor.{ActorLogging, ActorRef, FSM}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import org.zalando.znap.config.{Config, NakadiTarget}
import org.zalando.znap.nakadi.Messages.Ack
import org.zalando.znap.nakadi.NakadiReaderWorker._
import org.zalando.znap.nakadi.objects.{Cursor, EventBatch}
import org.zalando.znap.utils.{Json, UnexpectedMessageException}

/**
  * Actor that consumes events from Nakadi.
  */
class NakadiReaderWorker(partition: String,
                         offsetOpt: Option[String],
                         target: NakadiTarget,
                         config: Config,
                         tokens: NakadiTokens) extends FSM[State, Data] with ActorLogging {

  import NakadiReader._

  implicit val materializer = ActorMaterializer(ActorMaterializerSettings(context.system))

  override def preStart(): Unit = {
    val offset = offsetOpt.getOrElse("BEGIN")
    log.info(s"Start consuming partition $partition from offset $offset")

    val uri = s"/event-types/${target.eventType}/events" + "?stream_timeout=0"

    val authorizationHeader = new Authorization(OAuth2BearerToken(tokens.get()))
    val xNakadiCursor = new XNakadiCursors(partition, offset)
    val headers = List(authorizationHeader, xNakadiCursor)

    val nakadiConnectionFlow =
      if (target.schema.equals("https")) {
        Http(context.system).outgoingConnectionHttps(target.host, target.port)
      } else {
        Http(context.system).outgoingConnection(target.host, target.port)
      }
    val sink = Sink.actorRefWithAck(self, Init, Ack, StreamCompleted)

    Source.single(HttpRequest(HttpMethods.GET, uri, headers))
      .via(nakadiConnectionFlow)
      .flatMapConcat {
        case HttpResponse(StatusCodes.OK, _, entity, _) =>
          entity.withSizeLimit(config.HttpStreamingMaxSize)
            .dataBytes
            .scan("")((acc, curr) => if (acc.contains("\n")) curr.utf8String else acc + curr.utf8String)
            .filter(_.contains("\n"))
            .map {(x) => Json.read[EventBatch](x)}

        // If partitions are cleared on the backend, the local offset might become further.
        // To prevent this, we throw the special exception.
        case response@HttpResponse(StatusCodes.PreconditionFailed, _, entity, _) =>
          val offsetUnavailableRegex = s"""^offset\\s\\d+\\sfor\\spartition\\s$partition\\sis\\sunavailable$$""".r
          entity.dataBytes.fold("")(_ + " " + _.utf8String).map { content =>
            val preconditionFailedContent = Json.read[PreconditionFailedContent](content)
            assert(preconditionFailedContent.status == StatusCodes.PreconditionFailed.intValue)

            if (offsetUnavailableRegex.findFirstMatchIn(preconditionFailedContent.detail).isDefined) {
              throw new InvalidOffsetException(partition, offset)
            } else {
              val msg = s"Unknown response on getting events from partition $partition: $response $content"
              throw new Exception(msg)
            }
          }

        case unknownResponse: HttpResponse =>
          unknownResponse.entity.dataBytes.map { bs =>
            val msg = s"Unknown response on getting events from partition $partition: $unknownResponse ${bs.utf8String}"
            throw new Exception(msg)
          }
      }
      .runWith(sink)
  }

  startWith(WaitingForInit, Uninitialized)

  when(WaitingForInit)(handleBefore orElse[Event, State] {
    case Event(Init, Uninitialized) =>
      sender() ! Ack
      goto(WaitingForBatch) using Initialized(sender())
  })

  when(WaitingForBatch)(handleBefore orElse[Event, State] {
    case Event(eventBatch: EventBatch, Initialized(_)) =>
      checkCursor(eventBatch.cursor, eventBatch.events.isDefined)

      context.parent ! eventBatch
      goto(WaitingForResponseFromParent)

  })

  when(WaitingForResponseFromParent)(handleBefore orElse[Event, State] {
    case Event(Ack, Initialized(stream)) =>
      stream ! Ack
      goto(WaitingForBatch)

  })

  whenUnhandled {
    case Event(unexpected, _) =>
      throw new UnexpectedMessageException(unexpected)
  }

  def handleBefore: StateFunction = {
    case Event(StreamCompleted, _) =>
      throw new StreamCompletedException

    case Event(scala.util.Failure(throwable), _) =>
      throw throwable
    case Event(akka.actor.Status.Failure(throwable), _) =>
      throw throwable
  }

  private def checkCursor(cursor: Cursor, increase: Boolean): Unit = {
    assert(cursor.partition == partition)
  }
}

object NakadiReaderWorker {
  sealed trait State
  case object WaitingForInit extends State
  case object WaitingForBatch extends State
  case object WaitingForResponseFromParent extends State

  sealed trait Data
  case object Uninitialized extends Data
  final case class Initialized(stream: ActorRef) extends Data

  case object Init
  case object StreamCompleted

  @JsonIgnoreProperties(ignoreUnknown = true)
  final case class PreconditionFailedContent(`type`: String,
                                             title: String,
                                             status: Int,
                                             detail: String)
}
