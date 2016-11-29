/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.service

import akka.actor.{ActorRef, ActorSystem}
import akka.util.Timeout
import org.zalando.znap._
import org.zalando.znap.restapi.EntityReaders

import scala.concurrent.Future

class EntityReaderService(actorRoot: ActorRef) {
  import EntityReaderService._
  import akka.pattern.ask
  import scala.concurrent.duration._

  def getEntity(targetId: TargetId, key: String)
               (implicit actorSystem: ActorSystem): Future[GetEntityCommandResult] = {
    implicit val ec = actorSystem.dispatcher
    getActor(actorSystem, targetId).flatMap { ref =>
      implicit val askTimeout = Timeout(10.seconds)
      ref.ask(GetEntityCommand(key)).mapTo[GetEntityCommandResult]
    }
  }

  private def getActor(actorSystem: ActorSystem, targetId: TargetId): Future[ActorRef] = {
    implicit val resolveTimeout = 10.seconds
    val entityReaderSelection = actorSystem.actorSelection(
      actorRoot.path / EntityReaders.name / targetId
    )
    entityReaderSelection.resolveOne(resolveTimeout)
  }
}

object EntityReaderService {
  final case class GetEntityCommand(key: String)

  sealed trait GetEntityCommandResult
  case class NoEntityFound(key: String) extends GetEntityCommandResult
  final case class PlainEntity(key: String, value: String) extends GetEntityCommandResult
  final case class GzippedEntity(key: String, value: Array[Byte]) extends GetEntityCommandResult
  case object ProvisionedThroughputExceeded extends GetEntityCommandResult
}