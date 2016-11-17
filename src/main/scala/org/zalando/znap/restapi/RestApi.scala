package org.zalando.znap.restapi

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.http.scaladsl.coding.Gzip
import akka.http.scaladsl.model.{HttpEntity, _}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directive0, Route, StandardRoute}
import akka.http.scaladsl.unmarshalling.{Unmarshaller, _}
import akka.stream.ActorMaterializer
import akka.util.ByteString
import com.fasterxml.jackson.core.JsonProcessingException
import org.zalando.znap._
import org.zalando.znap.config.{Config, SnapshotTarget}
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

  private val targets: Map[TargetId, SnapshotTarget] =
    Config.Pipelines.flatMap(p => p.targets).map(t => t.id -> t).toMap

  private val measureLatencyDirectives = targets.keys.map { id =>
    id -> new MeasureLatencyDirective(id)(actorSystem)
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

    // Change the snapshot dumping status.
    val routeChangeDumpStatus =
      path("dumps" / Segment) {
        (dumpUid: String) => {
          patch {
            entity(as[PatchDumpBody]) { patchDumpBody =>
              patchDump(dumpUid, patchDumpBody)
            }
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
      routeChangeDumpStatus ~
      routeHealthCheck
  }

  private def getSnapshotList: StandardRoute = {
    val targetIds = targets.keys.toList
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
    val result = DumpKeysService.getDumpStatus(dumpUid).map { status =>
      dumpStatusToResponse(dumpUid)(status)
    }
    complete(result)
  }

  private def patchDump(dumpUid: String, patchDumpBody: PatchDumpBody): StandardRoute = {
    val result = patchDumpBody.status.toUpperCase() match {
      case DumpStatusAborted =>
        DumpKeysService.abortDump(dumpUid).map { status =>
          dumpStatusToResponse(dumpUid)(status)
        }

      case unsupported =>
        val contentType = MediaTypes.`application/json`
        val responseString = Json.createObject(
          "message" -> s"""Dump status "$unsupported" is not supported."""
        ).toString
        Future.successful(HttpResponse(
          StatusCodes.BadRequest,
          entity = HttpEntity(contentType, responseString)
        ))
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

  private val DumpStatusRunning = "RUNNING"
  private val DumpStatusFinishedSuccessfully = "FINISHED_SUCCESSFULLY"
  private val DumpStatusAborted = "ABORTED"
  private val DumpStatusFailed = "FAILED"

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
        timer.timeFuture {
          f()(ctx)
        }
      }
    }
  }

  case class PatchDumpBody(status: String)

  implicit val rawIntFromEntityUnmarshaller: FromEntityUnmarshaller[PatchDumpBody] =
    Unmarshaller.withMaterializer {
      implicit ex ⇒ implicit mat ⇒ entity: HttpEntity ⇒
        entity.dataBytes
          .runFold(ByteString.empty)(_ ++ _) // concat the entire request body
          .map { x =>
            val str = x.utf8String
          if (str.isEmpty) {
            throw Unmarshaller.NoContentException
          } else {
            try {
              Json.read[PatchDumpBody](str)
            } catch {
              case e: JsonProcessingException =>
                throw new IllegalArgumentException("", e)
            }
          }
        }
    }

  private def dumpStatusToResponse(dumpUid: String)
                                  (dumpStatus: dumps.DumpStatus): HttpResponse = {
    dumpStatus match {
      case dumps.DumpRunning =>
        val contentType = MediaTypes.`application/json`
        val responseString = Json.createObject(
          "status" -> DumpStatusRunning,
          "message" -> s"Dump is running"
        ).toString
        HttpResponse(
          StatusCodes.OK,
          entity = HttpEntity(contentType, responseString)
        )

      case dumps.DumpFinishedSuccefully =>
        val contentType = MediaTypes.`application/json`
        val responseString = Json.createObject(
          "status" -> DumpStatusFinishedSuccessfully,
          "message" -> s"Dump finished successfully"
        ).toString
        HttpResponse(
          StatusCodes.OK,
          entity = HttpEntity(contentType, responseString)
        )

      case dumps.DumpAborted =>
        val contentType = MediaTypes.`application/json`
        val responseString = Json.createObject(
          "status" -> DumpStatusAborted,
          "message" -> s"Dump aborted"
        ).toString
        HttpResponse(
          StatusCodes.OK,
          entity = HttpEntity(contentType, responseString)
        )

      case dumps.DumpFailed(errorMessage) =>
        val contentType = MediaTypes.`application/json`
        val responseString = Json.createObject(
          "status" -> DumpStatusFailed,
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
  }
}
