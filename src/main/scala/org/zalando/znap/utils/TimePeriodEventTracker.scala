/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.utils

import java.time.{Duration, ZonedDateTime}

/**
  * Class that tracks amount of events (like errors) in some period of time.
  *
  * @param period period of time to track.
  * @param maxEventAmount max amount of events that is allowed in the period of time.
  */
class TimePeriodEventTracker(val maxEventAmount: Int, val period: Duration) {
  private[this] var lastEventsTimes = List.empty[ZonedDateTime]

  /**
    * Register event and return if the limit is exceeded.
    *
    * @param time time of the event.
    * @return if event limit is exceeded.
    */
  def registerEvent(time: ZonedDateTime): Boolean = {
    lastEventsTimes = (time :: lastEventsTimes).take(maxEventAmount + 1)
    isTooManyEvents
  }

  /**
    * Checks if event limit is exceeded.
    *
    * @return if event limit is exceeded.
    */
  def isTooManyEvents: Boolean = {
    val now = ZonedDateTime.now()
    val eventsInTimePeriod = lastEventsTimes.count { dt =>
      java.time.Duration.between(dt, now).compareTo(period) < 0
    }
    eventsInTimePeriod > maxEventAmount
  }
}
