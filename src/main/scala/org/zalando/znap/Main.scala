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
import org.zalando.scarl.Supervisor.Specs
import org.zalando.scarl.{RootSupervisor, ScarlSupervisor}
import org.zalando.znap.config._
import org.zalando.znap.dump.DumpManager
import org.zalando.znap.source.nakadi.{NakadiTokens, OAuth}
import org.zalando.znap.pipeline.PipelineManager
import org.zalando.znap.restapi.Httpd

import scala.concurrent.duration._


object Main extends App {
  private val uid = "znap"

  val logger = LoggerFactory.getLogger(Main.getClass)
  logger.info(s"Application instance started with ID ${Config.ApplicationInstanceId}")

  // Init config.
  Config

  // Bootstrap directory structure if a disk destination exists.
  if (Config.Targets.exists(_.destination.isInstanceOf[DiskDestination])) {
    Bootstrapper.bootstrap()
  }

  val tokens = new NakadiTokens()

  implicit val actorSystem = ActorSystem(uid)

  actorSystem.registerOnTermination {
    LoggerFactory.getLogger(Main.getClass).info("Stopping token refreshing")
    tokens.stop()
  }

  actorSystem.rootSupervisor(
    Specs(uid, Props(classOf[SubSystemsSupervisor]))
  )

  actorSystem.actorOf(Props(classOf[PipelineManager], tokens))

  actorSystem.actorOf(Props(classOf[DumpManager], tokens), DumpManager.name)
}

class SubSystemsSupervisor extends RootSupervisor {
  override def supervisorStrategy = strategyOneForOne(3, 2.hours)

  def specs = Seq(
      Specs("httpd", Props[Httpd])
  )
}
