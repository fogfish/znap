/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.restapi

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpResponse

//import akka.http.scaladsl.marshalling._
import akka.http.scaladsl.server.Directives._
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._

/** rest api - snapshot entity
  *
  */
trait HttpdSnapshotEntity {
  def sys: ActorSystem
  implicit val timeout: Timeout = 30 seconds

  def apiSnapshotEntity() = {
    path("snapshots" / Segment / "entities" / Segment) {
      (snap: String, key: String) => {
        get {
          complete {
            (sys.actorSelection(s"/user/$snap/entity") ? key).mapTo[Option[String]]
          }
        }
      }
    }
  }

}
