/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.config

import java.net.URI

sealed trait SnapshotSource {

}

final case class NakadiSource(uri: URI,
                              eventType: String,
                              eventClass: String) extends SnapshotSource {
  val id: String = s"${eventType}_$eventClass"
}

sealed trait SnapshotDestination {

}

final case class DynamoDBDestination(url: URI) extends SnapshotDestination

final case class SnapshotTarget(source: SnapshotSource,
                                destination: SnapshotDestination,
                                key: List[String],
                                compress: Boolean) {
  val id: String = source match {
    case nakadiSource: NakadiSource => nakadiSource.id
  }
}
