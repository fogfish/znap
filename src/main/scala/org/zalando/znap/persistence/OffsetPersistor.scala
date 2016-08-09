/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.persistence

import org.zalando.znap.nakadi.objects.Cursor

import scala.concurrent.Future

trait OffsetPersistor {
  def persist(cursor: Cursor): Future[Unit]
}
