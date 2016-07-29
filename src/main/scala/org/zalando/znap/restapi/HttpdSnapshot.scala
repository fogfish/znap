package org.zalando.znap.restapi

import akka.actor.ActorSystem
import akka.http.scaladsl.marshalling._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.RequestContext

/** rest api - list of available snapshot
  *
  */
trait HttpdSnapshot {
  def sys: ActorSystem

  def apiSnapshot() = {
    path("snapshots") {
      get {
        completeWith[String](implicitly[ToResponseMarshaller[String]]) {
          commit =>
            sys.actorSelection("/user/snapshot") ! commit
        }
      }
    }
  }

}
