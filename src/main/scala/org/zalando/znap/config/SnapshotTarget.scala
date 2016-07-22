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

// TODO: name eventType is confusing, this is name of stream
// TODO: think to replace with ConfigStream
final case class NakadiTarget(
  schema: String,
  host: String,
  port: Int,
  eventType: String) extends SnapshotTarget {
  override val id: String = eventType
}
