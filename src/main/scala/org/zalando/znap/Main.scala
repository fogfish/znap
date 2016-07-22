/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap

import akka.actor.{ActorSystem, Props}
import org.zalando.znap.nakadi.{NakadiTargetSnapshotter, NakadiTokens}
import org.slf4j.LoggerFactory
import org.zalando.znap.config.{Config, NakadiTarget}

object Main extends App {
  val config = new Config()

  val logger = LoggerFactory.getLogger(Main.getClass)
  logger.info(s"Application instance started with ID ${config.ApplicationInstanceId}")

  new Bootstrapper(config).bootstrap()

  val tokens = new NakadiTokens(config)

  implicit val actorSystem = ActorSystem()

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
}
