/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.config

trait SnapshotTarget {
  val id: String
}

final case class NakadiTarget(host: String,
                        port: Int,
                        secureConnection: Boolean,
                        eventType: String) extends SnapshotTarget {
  override val id: String = eventType
}