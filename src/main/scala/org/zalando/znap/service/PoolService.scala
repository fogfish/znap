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
