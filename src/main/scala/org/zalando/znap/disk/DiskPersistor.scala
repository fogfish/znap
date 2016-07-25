/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.disk

import java.io.{File, IOException}

import akka.actor.{Props, Actor, ActorLogging}
import org.zalando.znap.config.{Config, SnapshotTarget}
import org.zalando.znap.nakadi.objects.EventBatch
import org.zalando.znap.objects.Partition
import org.zalando.znap.utils.{Json, NoUnexpectedMessages}
import org.zalando.znap.nakadi.objects.Event

class DiskPersistor(target: SnapshotTarget,
                    config: Config) extends Actor
    with NoUnexpectedMessages with ActorLogging {

  import DiskPersistor._

  private val instanceDir = new File(config.Paths.WorkingDirectory, config.ApplicationInstanceId)
  private val workingSnapshotDirectory = new File(instanceDir, target.id)

  private val hfs = context.actorOf(Props(new HashFS(workingSnapshotDirectory)))

  override def receive: Receive = {
    case InitCommand =>
      try {
        if (workingSnapshotDirectory.mkdir()) {
          log.info(s"Working snapshot directory for target $target ${workingSnapshotDirectory.getAbsoluteFile} created")
        } else {
          val message = s"Can't create working snapshot directory ${workingSnapshotDirectory.getAbsoluteFile} for target $target"
          log.error(message)
          throw new DiskException(message)
        }

        val snapshotDirectory = new File(config.Paths.SnapshotsDirectory, target.id)
        if (!snapshotDirectory.exists()) {
          log.info(s"No last snapshot for target $target: snapshot directory ${snapshotDirectory.getAbsoluteFile} doesn't exists")
        } else {
          // TODO implement snapshot restoration
          // Including snapshot consistency checks (partition files must exist, SHA-hashes, etc.)
          ???
        }
      } catch {
        case ex: IOException =>
          throw new DiskException(ex)
      }

    case AcceptPartitionsCommand(partitions) =>
      var partitionAndLastOffsetList: List[PartitionAndLastOffset] = Nil

      try {
        partitions.foreach { p =>
          val partitionFile = new File(workingSnapshotDirectory, s"partition_${p.partition}")

          if (partitionFile.createNewFile()) {
            log.info(s"Partition file $partitionFile created")
            partitionAndLastOffsetList =
              PartitionAndLastOffset(p.partition, None) :: partitionAndLastOffsetList
          } else {
            log.debug(s"Partition file $partitionFile exists, checking offsets")
            // TODO actually check offsets

            val lastOffset: String = ???

            partitionAndLastOffsetList =
              PartitionAndLastOffset(p.partition, Some(lastOffset)) :: partitionAndLastOffsetList
          }
        }
      } catch {
        case ex: IOException =>
          throw new DiskException(ex)
      }

      sender() ! PartitionsAccepted(partitionAndLastOffsetList)

    case event: EventBatch =>
      event.events foreach {_ foreach persist}
  }

  private
  def persist(x: Event): Unit =
    x.dataOp match {
      case "ARTICLE_UPDATE" =>
        //@todo: parametrize id discovery
        val id = x.data.get("sku").textValue()
        hfs forward HashFS.Put(id, Json.write(x))
    }

}

object DiskPersistor {
  case object InitCommand

  final case class AcceptPartitionsCommand(partitions: List[Partition])
  final case class PartitionAndLastOffset(partition: String, lastOffset: Option[String])
  final case class PartitionsAccepted(partitionAndLastOffsetList: List[PartitionAndLastOffset])

  final case class PersistCommand(key: String, value: String)
  case object Persisted
}
