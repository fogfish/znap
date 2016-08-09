/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.persistence

import org.zalando.znap.nakadi.objects.NakadiEvent

import scala.concurrent.Future

trait EventsPersistor {
  def persist(events: List[NakadiEvent]): Future[Unit]
}
