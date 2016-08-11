/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.config

import java.net.URI

import org.zalando.znap.source.nakadi.OAuth

sealed trait SnapshotSource {

}

final case class NakadiSource(uri: URI,
                              eventType: String,
                              eventClass: String) extends SnapshotSource {
  val id: String = s"${eventType}_$eventClass"
}

sealed trait SnapshotDestination {

}

final case class DynamoDBDestination(uri: URI,
                                     tableName: String,
                                     offsetsTableName: String) extends SnapshotDestination
final case class DiskDestination() extends SnapshotDestination

sealed trait Signalling

final case class SqsSignalling(uri: URI) extends Signalling

final case class SnapshotTarget(id: String,
                                source: SnapshotSource,
                                destination: SnapshotDestination,
                                signalling: Option[Signalling],
                                key: List[String],
                                compress: Boolean) {
}
