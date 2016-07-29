/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.restapi

import akka.actor.ActorSystem
import akka.http.scaladsl.marshalling._
import akka.http.scaladsl.server.Directives._

/** rest api - snapshot entity
  *
  */
trait HttpdSnapshotEntity {
  def sys: ActorSystem

  def apiSnapshotEntity() = {
    path("snapshots" / Segment / "entities" / Segment) {
      (snap: String, key: String) => {
        get {
          completeWith[String](implicitly[ToResponseMarshaller[String]]) {
            commit =>
              sys.actorSelection("/user/snapshot-entity") ! commit
          }
        }
      }
    }
  }

}
