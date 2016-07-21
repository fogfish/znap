/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.utils

import akka.actor.SupervisorStrategy.{Decider, Escalate}
import akka.actor.{AllForOneStrategy, SupervisorStrategy, SupervisorStrategyConfigurator}

import scala.concurrent.duration.Duration

/**
  * Supervision strategy that escalates every fail.
  */
class EscalateEverythingSupervisorStrategy(maxNrOfRetries: Int = -1,
                                           withinTimeRange: Duration = Duration.Inf,
                                           loggingEnabled: Boolean  = true)
  extends AllForOneStrategy(maxNrOfRetries, withinTimeRange, loggingEnabled)(
    EscalateEverythingSupervisorStrategy.EscalateEverythingDecider
  )

object EscalateEverythingSupervisorStrategy {
  private val EscalateEverythingDecider: Decider = {
    case _: Throwable => Escalate
  }
}

class EscalateEverythingSupervisorStrategyConfigurator extends SupervisorStrategyConfigurator {
  override def create(): SupervisorStrategy = new EscalateEverythingSupervisorStrategy
}
