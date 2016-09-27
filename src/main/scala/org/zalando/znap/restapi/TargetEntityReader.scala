/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.restapi

import akka.actor.SupervisorStrategy.{Escalate, Stop}
import akka.actor.{Actor, ActorLogging, OneForOneStrategy, Props, SupervisorStrategy}
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import org.zalando.znap.config.{Config, DynamoDBDestination, SnapshotTarget}
import org.zalando.znap.service.EntityReaderService
import org.zalando.znap.utils.{NoUnexpectedMessages, ThrowableUtils}

import scala.util.control.NonFatal

class TargetEntityReader(snapshotTarget: SnapshotTarget) extends Actor with NoUnexpectedMessages with ActorLogging {
  import EntityReaderService._

  private val dynamoDBDestination = snapshotTarget.destination.asInstanceOf[DynamoDBDestination]

  private val client = new AmazonDynamoDBClient()
  client.withEndpoint(dynamoDBDestination.uri.toString)

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case NonFatal(ex)  =>
      log.error(s"Entity reader ${sender()} failed with error, stopping ${ThrowableUtils.getStackTraceString(ex)}")
      Stop
    case _ =>
      Escalate
  }

  override def receive: Receive = {
    case c: GetEntityCommand =>
      val props = DynamoDBEntityReader.props(snapshotTarget.id, client, dynamoDBDestination)
        .withDispatcher(Config.Akka.DynamoDBDispatcher)
      val reader = context.actorOf(props)
      reader.forward(c)
  }
}

object TargetEntityReader {
  def props(snapshotTarget: SnapshotTarget): Props = {
    Props(classOf[TargetEntityReader], snapshotTarget)
  }
}