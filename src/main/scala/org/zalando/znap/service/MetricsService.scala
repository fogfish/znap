/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.service

import akka.actor.{ActorRef, ActorSystem}
import org.zalando.znap.metrics.MetricsCollector

import scala.concurrent.Future

object MetricsService {
  import scala.concurrent.duration._

  def sendGetEntityLatency(latency: Long)
                          (implicit actorSystem: ActorSystem): Unit = {
    implicit val ec = actorSystem.dispatcher
    getActor(actorSystem).map { ref =>
      ref ! MetricsCollector.GetEntityLatency(latency)
    }
  }

  def sendGetEntityFromDynamoLatency(latency: Long)
                                    (implicit actorSystem: ActorSystem): Unit = {
    implicit val ec = actorSystem.dispatcher
    getActor(actorSystem).map { ref =>
      ref ! MetricsCollector.GetEntityFromDynamoLatency(latency)
    }
  }

  private def getActor(actorSystem: ActorSystem): Future[ActorRef] = {
    implicit val resolveTimeout = 10.seconds
    val metricsCollectorSelection = actorSystem.actorSelection(s"/user/${MetricsCollector.name}")
    metricsCollectorSelection.resolveOne(resolveTimeout)
  }
}
