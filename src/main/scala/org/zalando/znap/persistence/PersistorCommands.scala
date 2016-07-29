/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.persistence

import org.zalando.znap.objects.Partition

object PersistorCommands {
  case object Init

  final case class AcceptPartitions(partitions: List[Partition])
  final case class PartitionAndLastOffset(partition: String, lastOffset: Option[String])
  final case class PartitionsAccepted(partitionAndLastOffsetList: List[PartitionAndLastOffset])
}
