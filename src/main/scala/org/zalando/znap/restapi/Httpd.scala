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
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.StandardRoute
import akka.stream.ActorMaterializer
import akka.util.{ByteString, Timeout}
import org.zalando.znap.config.Config
import org.zalando.znap.dumps
import org.zalando.znap.dumps.DumpManager
import org.zalando.znap.service.{DumpKeysService, SnapshotService}
import org.zalando.znap.utils.Json

import scala.concurrent.Future

/** REST API specification
  *
  */
class Httpd extends Actor with ActorLogging {

  import akka.pattern.ask

  import scala.concurrent.duration._

  private implicit val system = context.system
  private implicit val materializer = ActorMaterializer()
  private implicit val ec = context.dispatcher

  private val targets = Config.Targets.map(t => t.id -> t).toMap

  val routes = {
    // List of all available snapshots.
    val apiSnapshotList =
      path("snapshots") {
        get {
          getSnapshotList
        }
      }

    // Get a whole snapshot.
    val apiSnapshot =
      path("snapshots" / Segment) {
        (targetId: String) => {
          get {
            encodeResponseWith(Gzip) {
              getSnapshot(targetId)
            }
          }
        }
      }

    val initiateDumpApi =
      path("snapshots" / Segment / "dump") {
        (targetId: String) => {
          post {
            startDump(targetId)
          }
        }
      }

    // Get an entity from a snapshot.
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

    val getDumpStatusApi =
      path("dumps" / Segment) {
        (dumpUid: String) => {
          get {
            getDumpStatus(dumpUid)
          }
        }
      }

    val healthCheck =
      path("health" / "ping") {
        get {
          complete("ok")
        }
      }

    apiSnapshotList ~
      apiSnapshotEntity ~
      apiSnapshot ~
      initiateDumpApi ~
      getDumpStatusApi ~
      healthCheck
  }

  private def getSnapshotList: StandardRoute = {
    val targetIds = Config.Targets.map(_.id)
    val responseString = Json.write(targetIds)
    val contentType = MediaTypes.`application/json`
    val response = HttpResponse(entity = HttpEntity(contentType, responseString))
    complete(response)
  }

  private def startDump(targetId: String): StandardRoute = {
    targets.get(targetId) match {
      case Some(target) =>
        val result = DumpKeysService.dump(target)(context.system).map {
          case DumpManager.DumpStarted(dumpUid) =>
            val responseString = Json.createObject("dumpUid" -> dumpUid)
              .toString
            val contentType = MediaTypes.`application/json`
            HttpResponse(
              StatusCodes.Accepted,
              entity = HttpEntity(contentType, responseString)
            )

          case DumpManager.AnotherDumpAlreadyRunning(dumpUid) =>
            val responseString = Json.createObject(
              "message" -> s"Another dump for target $targetId is running",
              "dumpUid" -> dumpUid
            ).toString
            val contentType = MediaTypes.`application/json`
            HttpResponse(
              StatusCodes.Conflict,
              entity = HttpEntity(contentType, responseString)
            )

          case DumpManager.DumpingNotConfigured =>
            val responseString = Json.createObject(
              "message" -> s"Dumping for target $targetId is not configured"
            ).toString
            val contentType = MediaTypes.`application/json`
            HttpResponse(
              StatusCodes.BadRequest,
              entity = HttpEntity(contentType, responseString)
            )
        }
        complete(result)

      case None =>
        complete(unknownTargetResponse(targetId))
    }
  }

  private def getDumpStatus(dumpUid: String): StandardRoute = {
    val result = DumpKeysService.getDumpStatus(dumpUid)(context.system).map {
      case dumps.DumpRunning =>
        val contentType = MediaTypes.`application/json`
        val responseString = Json.createObject(
          "status" -> "RUNNING",
          "message" -> s"Dump is running"
        ).toString
        HttpResponse(
          StatusCodes.OK,
          entity = HttpEntity(contentType, responseString)
        )

      case dumps.DumpFinishedSuccefully =>
        val contentType = MediaTypes.`application/json`
        val responseString = Json.createObject(
          "status" -> "FINISHED_SUCCESSFULLY",
          "message" -> s"Dump finished successfully"
        ).toString
        HttpResponse(
          StatusCodes.OK,
          entity = HttpEntity(contentType, responseString)
        )

      case dumps.DumpFailed(errorMessage) =>
        val contentType = MediaTypes.`application/json`
        val responseString = Json.createObject(
          "status" -> "FAILED",
          "message" -> s"""Dump failed with error message: "$errorMessage""""
        ).toString
        HttpResponse(
          StatusCodes.OK,
          entity = HttpEntity(contentType, responseString)
        )

      case dumps.UnknownDump =>
        val contentType = MediaTypes.`application/json`
        val responseString = Json.createObject(
          "message" -> s"Unknown dump $dumpUid"
        ).toString
        HttpResponse(
          StatusCodes.NotFound,
          entity = HttpEntity(contentType, responseString)
        )
    }
    complete(result)
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
              val contentType = MediaTypes.`application/json`
              val responseString = s"""{"message": "Unknown key $targetId"}"""
              HttpResponse(
                StatusCodes.NotFound,
                entity = HttpEntity(contentType, responseString)
              )
          }
        }

      case None =>
        complete(unknownTargetResponse(targetId))
    }
  }

  private def unknownTargetResponse(targetId: String): HttpResponse = {
    val contentType = MediaTypes.`application/json`
    val responseString = s"""{"message": "Unknown target $targetId"}"""
    HttpResponse(
      StatusCodes.NotFound,
      entity = HttpEntity(contentType, responseString)
    )
  }


  private def getSnapshot(targetId: String): StandardRoute = {
    targets.get(targetId) match {
      case Some(target) =>
        val keySource = SnapshotService
          .getSnapshotKeys(target)
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


  override def preStart(): Unit = {
    http = Http().bindAndHandle(routes, "0.0.0.0", 8080)
  }

  override def postStop(): Unit = {
    http.foreach(_.unbind())
  }

  def receive: Receive = {
    case x: Any =>
      log.warning(s"unexpected message $x")
  }

}
