package org.zalando.znap.restapi

import java.util.concurrent.ConcurrentHashMap

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.http.scaladsl.coding.Gzip
import akka.http.scaladsl.model.{HttpEntity, _}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directive0, Route, StandardRoute}
import akka.stream.ActorMaterializer
import org.zalando.znap._
import org.zalando.znap.config.Config
import org.zalando.znap.dumps.DumpManager
import org.zalando.znap.metrics.Instrumented
import org.zalando.znap.service.{DumpKeysService, EntityReaderService}
import org.zalando.znap.utils.Json

import scala.concurrent.Future

class RestApi(actorRoot: ActorRef, actorSystem: ActorSystem) {
  import RestApi._

  private implicit val system = actorSystem
  private implicit val materializer = ActorMaterializer()
  private implicit val ec = actorSystem.dispatcher

  private val entityReaderService = new EntityReaderService(actorRoot)

  private val targets = Config.Targets.map(t => t.id -> t).toMap

  private val measureLatencyDirectives = Config.Targets.map { t =>
    t.id -> new MeasureLatencyDirective(t.id)(actorSystem)
  }.toMap

  private val routes = {
    // List of all available snapshots.
    val routeGetSnapshotList =
    path("snapshots") {
      get {
        getSnapshotList
      }
    }

    // Start dumping of a snapshot.
    val routeStartDump =
      path("snapshots" / Segment / "dump") {
        (targetId: TargetId) => {
          post {
            parameter('force_restart ? false) { forceRestart =>
              startDump(targetId, forceRestart)
            }
          }
        }
      }

    // Get the snapshot dumping status.
    val routeGetDumpStatus =
      path("dumps" / Segment) {
        (dumpUid: String) => {
          get {
            getDumpStatus(dumpUid)
          }
        }
      }


    // Get an entity from a snapshot.
    val routeGetSnapshotEntity =
    path("snapshots" / Segment / "entities" / Segment) {
      (targetId: TargetId, key: String) => {
        measureLatencyDirectives(targetId) {
          get {
            encodeResponseWith(Gzip) {
              getSnapshotEntity(targetId, key)
            }
          }
        }
      }
    }


    // Health check.
    val routeHealthCheck =
    path("health" / "ping") {
      get {
        complete("ok")
      }
    }

    routeGetSnapshotList ~
      routeGetSnapshotEntity ~
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

  private def startDump(targetId: TargetId, forceRestart: Boolean): StandardRoute = {
    targets.get(targetId) match {
      case Some(target) =>
        val result = DumpKeysService.dump(target, forceRestart).map {
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

      case dumps.DumpAborted =>
        val contentType = MediaTypes.`application/json`
        val responseString = Json.createObject(
          "status" -> "ABORTED",
          "message" -> s"Dump aborted"
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
        }.recover {
          case e: akka.pattern.AskTimeoutException =>
            val contentType = MediaTypes.`application/json`
            val responseString = s"""{"message": "Request took too long. Retry later"}"""
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

  private def unknownTargetResponse(targetId: TargetId): HttpResponse = {
    val contentType = MediaTypes.`application/json`
    val responseString = s"""{"message": "Unknown target $targetId"}"""
    HttpResponse(
      StatusCodes.NotFound,
      entity = HttpEntity(contentType, responseString)
    )
  }

  private var http: Future[Http.ServerBinding] = _

  def start(): Unit = {
    http = Http().bindAndHandle(routes, "0.0.0.0", 8080)
  }

  def stop(): Unit = {
    http.foreach(_.unbind())
  }
}

object RestApi {

  /**
    * Directive for measuring latencies of HTTP queries
    * and sending them to Dropwizard.
    */
  class MeasureLatencyDirective(targetId: TargetId)
                               (implicit actorSystem: ActorSystem) extends Directive0 with Instrumented {
    private implicit val executionContext = actorSystem.dispatcher

    private val timer = metrics.timer(s"get-entity-rest-$targetId")

    override def tapply(f: (Unit) => Route): Route = {
      ctx => {
        val timerCtx = timer.timerContext()
        val started = System.nanoTime()
        f()(ctx).map { result =>
          timerCtx.stop()
          result
        }
      }
    }
  }
}
