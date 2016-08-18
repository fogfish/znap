/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.restapi

import akka.actor.{Actor, ActorLogging}

class Httpd extends Actor with ActorLogging {

  private val api = new RestApi(self, context.system)

  override def preStart(): Unit = {
    context.actorOf(EntityReaders.props(), EntityReaders.name)
    api.start()
  }

  override def postStop(): Unit = {
    api.stop()
  }

  override def receive: Receive = {
    case x: Any =>
      log.warning(s"unexpected message $x")
  }
}

object Httpd {
  val name = "httpd"
}