/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap

import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.Connection
import akka.stream.scaladsl.Sink
import org.zalando.znap.nakadi.{NakadiTargetSnapshotter, NakadiTokens}
import org.slf4j.LoggerFactory
import org.zalando.znap.config.{Config, NakadiTarget}
import org.zalando.znap.restapi.Httpd
import org.zalando.znap.service.{SnapshotEntityService, SnapshotService}

object Main extends App {
  val config = new Config()

  implicit val logger = LoggerFactory.getLogger(Main.getClass)
  logger.info(s"Application instance started with ID ${config.ApplicationInstanceId}")

  new Bootstrapper(config).bootstrap()

  val tokens = new NakadiTokens(config)

  implicit val actorSystem = ActorSystem("znap")

  actorSystem.registerOnTermination {
    LoggerFactory.getLogger(Main.getClass).info("Stopping token refreshing")
    tokens.stop()
  }


  config.Targets.foreach {
    case nakadiTarget: NakadiTarget =>
      actorSystem.actorOf(
        Props(classOf[NakadiTargetSnapshotter], nakadiTarget, config, tokens)
      )
  }

  actorSystem.actorOf(SnapshotService.spec(), "snapshot")
  actorSystem.actorOf(SnapshotEntityService.spec(), "snapshot-entity")
  actorSystem.actorOf(Props[Httpd])

}
