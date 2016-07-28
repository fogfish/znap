package org.zalando.znap.restapi

import akka.actor.{Actor, ActorLogging}
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import scala.concurrent.Future
import akka.http.scaladsl.server.Directives._

/** REST API specification
  *
  */
class Httpd extends Actor with ActorLogging
  with HttpdSnapshotEntity
  with HttpdSnapshot {

  implicit val sys = context.system
  implicit val factory = ActorMaterializer()
  implicit val ec = context.dispatcher

  /** set of active routes */
  val routes =
    apiSnapshotEntity() ~
    apiSnapshot()

  var http: Future[Http.ServerBinding] = _


  override
  def preStart() = {
    http = Http().bindAndHandle(routes, "0.0.0.0", 8080)
  }

  override
  def postStop() = {
    http.map {_.unbind()}
  }

  def receive = {
    case x: Any =>
      log.warning(s"unexpected message $x")
  }
}
