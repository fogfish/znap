/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.restapi

import akka.actor.{Actor, ActorLogging}
import akka.http.scaladsl.Http
import akka.http.scaladsl.coding.Gzip
import akka.http.scaladsl.model.{HttpEntity, _}
import akka.stream.ActorMaterializer

import scala.concurrent.{ExecutionContext, Future}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.StandardRoute
import akka.util.{ByteString, Timeout}
import org.zalando.znap.config.Config
import org.zalando.znap.service.SnapshotService

/** REST API specification
  *
  */
class Httpd(config: Config) extends Actor with ActorLogging {

  import akka.pattern.ask
  import scala.concurrent.duration._

  private implicit val system = context.system
  private implicit val materializer = ActorMaterializer()
  private implicit val ec = context.dispatcher

  private val targets = config.Targets.map(t => t.id -> t).toMap

  /** set of active routes */
  val routes = {
    val apiSnapshot =
      path("snapshots" / Segment) {
        (targetId: String) =>
          get {
            encodeResponseWith(Gzip) {
              getSnapshot(targetId)
            }
          }
      }

    val apiSnapshotEntity =
      path("snapshots" / Segment / "entities" / Segment) {
        (targetId: String, key: String) => {
          get {
            encodeResponseWith(Gzip) {
              getSnapshotEntity(targetId, key)
            }
          }
        }
      }

    apiSnapshotEntity ~
      apiSnapshot
  }

  private def getSnapshotEntity(targetId: String, key: String): StandardRoute = {
    import scala.language.postfixOps

    implicit val timeout: Timeout = 30.seconds
    targets.get(targetId) match {
      case Some(target) =>
        complete {
          (system.actorSelection(s"/user/$targetId/entity") ? key).mapTo[Option[String]].map {
            case Some(str) =>
              HttpResponse(entity = HttpEntity(str))

            case None =>
              HttpResponse(StatusCodes.NotFound, entity = HttpEntity(s"Unknown key $key"))
          }
        }

      case None =>
        complete {
          HttpResponse(StatusCodes.NotFound, entity = HttpEntity(s"Unknown target $targetId"))
        }
    }
  }


  private def getSnapshot(targetId: String): StandardRoute = {
    targets.get(targetId) match {
      case Some(target) =>
        val keySource = SnapshotService
          .getSnapshotKeys(target, config)
          .map(serializeKey)

        val contentType = MediaTypes.`text/plain`.withCharset(HttpCharsets.`UTF-8`)
        val response = HttpResponse(entity = HttpEntity(contentType, keySource))
        complete(response)

      case None =>
        complete {
          HttpResponse(StatusCodes.NotFound, entity = HttpEntity(s"Unknown target $targetId"))
        }
    }
  }

  private val encoding  = "UTF-8"
  private val newLineBS = "\n".getBytes(encoding)
  private def serializeKey(key: String): ByteString = {
    val builder = ByteString.newBuilder
    builder.sizeHint((key.length * 1.5).toInt)
    builder.putBytes(key.getBytes(encoding))
    builder.putBytes(newLineBS)
    builder.result()
  }


  var http: Future[Http.ServerBinding] = _


  override def preStart() = {
    http = Http().bindAndHandle(routes, "0.0.0.0", 8080)
  }

  override def postStop() = {
    http.map {_.unbind()}
  }

  def receive = {
    case x: Any =>
      log.warning(s"unexpected message $x")
  }
}
