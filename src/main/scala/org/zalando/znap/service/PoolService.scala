/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.service

import akka.actor.SupervisorStrategy.Stop
import akka.actor.{Props, OneForOneStrategy, Actor}
import scala.concurrent.duration._

trait PoolService extends Actor {

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 0, withinTimeRange = 1 seconds) {
      case _: Exception => Stop
    }

  def receive = {
    case request: Any =>
      context.actorOf(props) forward request
  }

  def props: Props
}
