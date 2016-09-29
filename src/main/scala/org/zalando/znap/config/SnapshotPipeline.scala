/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.config

import java.net.URI

import org.zalando.znap.PipelineId

sealed trait SnapshotSource
case object EmptySource extends SnapshotSource
final case class NakadiSource(uri: URI,
                              eventType: String,
                              batchLimit: Option[Long],
                              compress: Boolean,
                              filter: Option[SourceFilter]) extends SnapshotSource

final case class SourceFilter(field: String,
                              values: Set[String])


sealed trait SnapshotDestination
case object EmptyDestination extends SnapshotDestination
final case class DynamoDBDestination(uri: URI,
                                     tableName: String,
                                     compress: Boolean) extends SnapshotDestination


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

sealed trait OffsetPersistence
case object EmptyOffsetPersistence extends OffsetPersistence
final case class DynamoDBOffsetPersistence(uri: URI,
                                           tableName: String) extends OffsetPersistence

sealed trait PublishType
object PublishType {
  case object KeysOnly extends PublishType
  case object EventsUncompressed extends PublishType
  case object EventsCompressed extends PublishType
}


final case class SnapshotPipeline(id: PipelineId,
                                  source: SnapshotSource,
                                  destination: SnapshotDestination,
                                  signalling: Option[Signalling],
                                  dumping: Option[Dumping],
                                  offsetPersistence: OffsetPersistence,
                                  key: List[String])
