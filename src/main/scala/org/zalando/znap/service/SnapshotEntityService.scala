/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.service

import akka.actor._
import org.zalando.scarl.Supervisor.Specs


object SnapshotEntityService {
  def spec(pid: ActorSelection) =
    Specs(classOf[SnapshotEntityService].getSimpleName, Props(classOf[SnapshotEntityService], pid))
}

class SnapshotEntityService(pid: ActorSelection) extends PoolService {
  import org.zalando.scarl.ScarlSelection

  override def props: Props =
    Props(classOf[SnapshotEntityRequest], pid.resolve())
}


class SnapshotEntityRequest(reader: ActorRef) extends Actor {
  var caller: ActorRef = _

  def receive = {
    case key: String =>
      reader ! DynamoDBEntityReader.GetEntityCommand(key)
      caller = sender()

    case DynamoDBEntityReader.Entity(_, entity) =>
      caller ! entity
      context.stop(self)
  }
}

