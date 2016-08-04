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
import org.zalando.znap.config.{Config, NakadiSource, SnapshotTarget}
import org.zalando.znap.nakadi.{NakadiTargetSnapshotterSup, NakadiTokens}
import org.zalando.znap.restapi.Httpd
import org.zalando.znap.service.SnapshotService
import org.zalando.znap.service.queue.QueueService

object Main extends App {
  val config = new Config()

  implicit val logger = LoggerFactory.getLogger(Main.getClass)
  logger.info(s"Application instance started with ID ${config.ApplicationInstanceId}")

//  new Bootstrapper(config).bootstrap()

  val tokens = new NakadiTokens(config)

  implicit val actorSystem = ActorSystem("znap")

  actorSystem.registerOnTermination {
    LoggerFactory.getLogger(Main.getClass).info("Stopping token refreshing")
    tokens.stop()
  }

  //
  // spawn ingress queue coordinator processes
  actorSystem.actorOf(QueueService.spec(config.Targets, config, tokens), QueueService.id)

  //
  // spawn ingress streams
  config.Targets.foreach {
    case target @ SnapshotTarget(source: NakadiSource, _, _, _) =>
      actorSystem.actorOf(
        Props(classOf[NakadiTargetSnapshotterSup], target, config, tokens),
        source.eventClass
      )
  }

  actorSystem.actorOf(SnapshotService.spec(), "snapshot")
  actorSystem.actorOf(Props[Httpd])

}
