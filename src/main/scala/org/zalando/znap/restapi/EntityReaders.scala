/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.restapi

import akka.actor.SupervisorStrategy.{Escalate, Restart}
import akka.actor.{Actor, ActorLogging, ActorRef, OneForOneStrategy, Props, SupervisorStrategy}
import org.zalando.znap._
import org.zalando.znap.config.{Config, DynamoDBDestination, SnapshotTarget}
import org.zalando.znap.utils.ThrowableUtils

import scala.util.control.NonFatal

class EntityReaders extends Actor with ActorLogging {

  private var entityReaders = Map.empty[TargetId, ActorRef]

  private def entityReaderSet: Set[ActorRef] = {
    entityReaders.values.toSet
  }

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case NonFatal(ex) if entityReaderSet.contains(sender()) =>
      log.error(s"Entity reader ${sender()} failed with error ${ThrowableUtils.getStackTraceString(ex)}, restarting")
      Restart
    case _ =>
      Escalate
  }

  override def preStart(): Unit = {
    Config.Pipelines.foreach { p =>
      p.targets.foreach(runTargetEntityReader)
    }
  }

  private def runTargetEntityReader(target: SnapshotTarget): Unit = {
    val ref = target.destination match {
      case _: DynamoDBDestination =>
        val props = TargetEntityReader.props(target)
        context.actorOf(props, target.id)
    }
    entityReaders += target.id -> ref
  }

  override def receive: Receive = {
    case x: Any =>
      log.warning(s"unexpected message $x")
  }
}

object EntityReaders {
  val name = "entity-readers"

  def props(): Props = {
    Props(classOf[EntityReaders])
  }
}
