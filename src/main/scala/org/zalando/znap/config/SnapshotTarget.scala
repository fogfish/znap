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

sealed trait SnapshotSource
case object EmptySource extends SnapshotSource
final case class NakadiSource(uri: URI,
                              eventType: String,
                              eventClass: String) extends SnapshotSource {
  val id: String = s"${eventType}_$eventClass"
}


sealed trait SnapshotDestination
case object EmptyDestination extends SnapshotDestination
final case class DynamoDBDestination(uri: URI,
                                     tableName: String,
                                     offsetsTableName: String) extends SnapshotDestination
final case class DiskDestination() extends SnapshotDestination


sealed trait Signalling
final case class SqsSignalling(uri: URI,
                               publishType: PublishType) extends Signalling
final case class KinesisSignalling(amazonRegion: String,
                                   stream: String,
                                   publishType: PublishType) extends Signalling


sealed trait Dumping
final case class SqsDumping(uri: URI,
                               publishType: PublishType) extends Dumping
final case class KinesisDumping(amazonRegion: String,
                                stream: String,
                                publishType: PublishType) extends Dumping


sealed trait PublishType
object PublishType {
  case object KeysOnly extends PublishType
  case object EventsUncompressed extends PublishType
  case object EventsCompressed extends PublishType
}


final case class SnapshotTarget(id: String,
                                source: SnapshotSource,
                                destination: SnapshotDestination,
                                signalling: Option[Signalling],
                                dumping: Option[Dumping],
                                key: List[String],
                                compress: Boolean) {
}
