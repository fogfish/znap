/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.service

import akka.actor.{ActorRef, Actor, Props}


object SnapshotEntityService {
  type Commit = (String) => Unit

  def spec(reader: ActorRef) =
    Props(classOf[SnapshotEntityService], reader)
}


class SnapshotEntityService(reader: ActorRef) extends PoolService {
  override def props: Props = Props(classOf[SnapshotEntityRequest], reader)
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

