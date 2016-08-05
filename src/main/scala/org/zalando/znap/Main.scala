/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap

import akka.actor.{ActorSystem, Props}
import org.slf4j.LoggerFactory
import org.zalando.scarl.RootSupervisor
import org.zalando.scarl.ScarlSupervisor
import org.zalando.scarl.Supervisor.Specs
import org.zalando.znap.config.{Config, NakadiSource, SnapshotTarget}
import org.zalando.znap.nakadi.{OAuth, NakadiTargetSnapshotter, NakadiTokens}
import org.zalando.znap.restapi.Httpd
import org.zalando.znap.service.SnapshotService
import org.zalando.znap.service.queue.QueueService
import org.zalando.znap.service.stream.StreamService
import scala.concurrent.duration._


object Main extends App {
  private val uid = "znap"

  Config

  implicit val logger = LoggerFactory.getLogger(Main.getClass)
  logger.info(s"Application instance started with ID ${Config.ApplicationInstanceId}")

  val tokens = new NakadiTokens()

  implicit val actorSystem = ActorSystem(uid)

  actorSystem.registerOnTermination {
    LoggerFactory.getLogger(Main.getClass).info("Stopping token refreshing")
    tokens.stop()
  }

  actorSystem.rootSupervisor(
    Specs(uid, Props(classOf[SubSystemsSupervisor], tokens))
  )
}

class SubSystemsSupervisor(oauth: OAuth) extends RootSupervisor {
  override
  def supervisorStrategy = strategyOneForOne(3, 2.hours)

  def specs = Seq(
      QueueService.spec(oauth),
      StreamService.spec(oauth),
      Specs("httpd", Props[Httpd])
  )
}
