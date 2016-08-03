/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.service

import akka.actor._

import scala.annotation.tailrec
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util._


object SnapshotEntityService {
  type Commit = (String) => Unit

  def spec(pid: ActorSelection) =
    Props(classOf[SnapshotEntityService], pid)
}


class SnapshotEntityService(pid: ActorSelection) extends PoolService {

  @tailrec
  private
  def resolve(pid: ActorSelection): ActorRef = {
    Try(Await.result(pid.resolveOne(5 second), Duration.Inf)) match {
      case Failure(_: ActorNotFound) =>
        resolve(pid)
      case Success(ref: ActorRef) =>
        ref
    }
  }

  override def props: Props =
    Props(classOf[SnapshotEntityRequest], resolve(pid))
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

