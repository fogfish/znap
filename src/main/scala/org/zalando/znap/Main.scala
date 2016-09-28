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
import org.zalando.znap.dumps.DumpManager
import org.zalando.znap.healthcheck.ProgressChecker
import org.zalando.znap.pipeline.PipelineManager
import org.zalando.znap.source.nakadi.NakadiTokens
import org.zalando.znap.restapi.Httpd

import scala.concurrent.duration._


object Main extends App {
  val logger = LoggerFactory.getLogger(Main.getClass)
  logger.info(s"Application instance started with ID ${Config.ApplicationInstanceId}")

  // Init config.
  Config

  val tokens = new NakadiTokens()

  implicit val actorSystem = ActorSystem("znap")

  actorSystem.registerOnTermination {
    LoggerFactory.getLogger(Main.getClass).info("Stopping token refreshing")
    tokens.stop()
  }

  actorSystem.actorOf(ProgressChecker.props(tokens), ProgressChecker.name)

  actorSystem.rootSupervisor(
    Specs(SubSystemsSupervisor.name, SubSystemsSupervisor.props())
  )

  actorSystem.actorOf(Props(classOf[PipelineManager], tokens))

  actorSystem.actorOf(DumpManager.props(tokens), DumpManager.name)
}

class SubSystemsSupervisor extends RootSupervisor {
  override def supervisorStrategy = strategyOneForOne(3, 2.hours)

  def specs = Seq(
      Specs(Httpd.name, Props[Httpd])
  )
}

object SubSystemsSupervisor {
  val name = "znap"

  def props(): Props = {
    Props(classOf[SubSystemsSupervisor])
  }
}