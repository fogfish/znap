package org.zalando.znap.restapi

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.http.scaladsl.coding.Gzip
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.StandardRoute
import akka.stream.ActorMaterializer
import akka.util.{ByteString, Timeout}
import org.zalando.znap._
import org.zalando.znap.config.Config
import org.zalando.znap.dumps.DumpManager
import org.zalando.znap.service.{DumpKeysService, SnapshotService}
import org.zalando.znap.utils.Json
import org.zalando.znap.service.EntityReaderService

import scala.concurrent.Future

class RestApi(actorRoot: ActorRef, actorSystem: ActorSystem) {
  import scala.concurrent.duration._

  private implicit val system = actorSystem
  private implicit val materializer = ActorMaterializer()
  private implicit val ec = actorSystem.dispatcher

  private val entityReaderService = new EntityReaderService(actorRoot)

  private val targets = Config.Targets.map(t => t.id -> t).toMap

  private val routes = {
    // List of all available snapshots.
    val routeGetSnapshotList =
    path("snapshots") {
      get {
        getSnapshotList
      }
    }

    // Get a whole snapshot.
    val routeGetSnapshot =
    path("snapshots" / Segment) {
      (targetId: TargetId) => {
        get {
          encodeResponseWith(Gzip) {
            getSnapshot(targetId)
          }
        }
      }
    }

    val routeStartDump =
      path("snapshots" / Segment / "dump") {
        (targetId: TargetId) => {
          post {
            startDump(targetId)
          }
        }
      }

    // Get an entity from a snapshot.
    val routeGetSnapshotEntity =
    path("snapshots" / Segment / "entities" / Segment) {
      (targetId: TargetId, key: String) => {
        get {
          encodeResponseWith(Gzip) {
            getSnapshotEntity(targetId, key)
          }
        }
      }
    }

    val routeGetDumpStatus =
      path("dumps" / Segment) {
        (dumpUid: String) => {
          get {
            getDumpStatus(dumpUid)
          }
        }
      }

    val routeHealthCheck =
      path("health" / "ping") {
        get {
          complete("ok")
        }
      }

    routeGetSnapshotList ~
      routeGetSnapshotEntity ~
      routeGetSnapshot ~
      routeStartDump ~
      routeGetDumpStatus ~
      routeHealthCheck
  }

  private def getSnapshotList: StandardRoute = {
    val targetIds = Config.Targets.map(_.id)
    val responseString = Json.write(targetIds)
    val contentType = MediaTypes.`application/json`
    val response = HttpResponse(entity = HttpEntity(contentType, responseString))
    complete(response)
  }

  private def startDump(targetId: TargetId): StandardRoute = {
    targets.get(targetId) match {
      case Some(target) =>
        val result = DumpKeysService.dump(target).map {
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
    val result = DumpKeysService.getDumpStatus(dumpUid).map {
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

  private def getSnapshotEntity(targetId: TargetId, key: String): StandardRoute = {
    import scala.language.postfixOps

    implicit val timeout: Timeout = 30.seconds
    targets.get(targetId) match {
      case Some(target) =>
        val result = entityReaderService.getEntity(targetId, key).map {
          case EntityReaderService.Entity(`key`, Some(str)) =>
            HttpResponse(entity = HttpEntity(str))

          case EntityReaderService.Entity(`key`, None) =>
            val contentType = MediaTypes.`application/json`
            val responseString = s"""{"message": "Unknown key $key"}"""
            HttpResponse(
              StatusCodes.NotFound,
              entity = HttpEntity(contentType, responseString)
            )

          case EntityReaderService.ProvisionedThroughputExceeded =>
            val contentType = MediaTypes.`application/json`
            val responseString = s"""{"message": "Provisioned throughput exceeded"}"""
            HttpResponse(
              StatusCodes.ServiceUnavailable,
              entity = HttpEntity(contentType, responseString)
            )
        }
        complete(result)

      case None =>
        complete(unknownTargetResponse(targetId))
    }
  }

  private def getSnapshot(targetId: TargetId): StandardRoute = {
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


  private def unknownTargetResponse(targetId: TargetId): HttpResponse = {
    val contentType = MediaTypes.`application/json`
    val responseString = s"""{"message": "Unknown target $targetId"}"""
    HttpResponse(
      StatusCodes.NotFound,
      entity = HttpEntity(contentType, responseString)
    )
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

  private var http: Future[Http.ServerBinding] = _

  def start(): Unit = {
    http = Http().bindAndHandle(routes, "0.0.0.0", 8080)
  }

  def stop(): Unit = {
    http.foreach(_.unbind())
  }
}
