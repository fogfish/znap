/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap

import akka.actor.{ActorSystem, Props}
import akka.stream.ActorMaterializer
import org.slf4j.LoggerFactory
import org.zalando.scarl.Supervisor.Specs
import org.zalando.scarl.{RootSupervisor, ScarlSupervisor}
import org.zalando.znap.config._
import org.zalando.znap.nakadi.{NakadiTokens, OAuth}
import org.zalando.znap.pipeline.PipelineManager
import org.zalando.znap.restapi.Httpd

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

  actorSystem.actorOf(Props(classOf[PipelineManager], tokens))
}

class SubSystemsSupervisor(oauth: OAuth) extends RootSupervisor {
  override def supervisorStrategy = strategyOneForOne(3, 2.hours)

  def specs = Seq(
//      QueueService.spec(oauth),
//      StreamService.spec(oauth),
      Specs("httpd", Props[Httpd])
  )
}
