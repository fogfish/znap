/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.disk

import java.io.{File, IOException}

import akka.actor.{Actor, ActorLogging, ActorRef, FSM, Props, Stash}
import org.zalando.znap.config.{Config, SnapshotTarget}
import org.zalando.znap.nakadi.Messages.Ack
import org.zalando.znap.nakadi.objects.{EventBatch, NakadiEvent}
import org.zalando.znap.objects.Partition
import org.zalando.znap.utils.{Json, UnexpectedMessageException}
import DiskPersistor.{Data, State}

class DiskPersistor(target: SnapshotTarget,
                    config: Config) extends FSM[State, Data] with Stash with ActorLogging {

  import DiskPersistor._

  private val instanceDir = new File(config.Paths.WorkingDirectory, config.ApplicationInstanceId)
  private val workingSnapshotDirectory = new File(instanceDir, target.id)

  private val hashFS = context.actorOf(Props(classOf[HashFS], workingSnapshotDirectory))
  private val offsetWriter = context.actorOf(Props(classOf[OffsetWriter], workingSnapshotDirectory))

  startWith(Initialization, NoData)

  when(Initialization) {
    case Event(InitCommand, NoData) =>
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
        } else if (snapshotDirectory.list().isEmpty) {
          log.info(s"No last snapshot for target $target: snapshot directory ${snapshotDirectory.getAbsoluteFile} is empty")
        } else {
          // TODO implement snapshot restoration
          // Including snapshot consistency checks (partition files must exist, SHA-hashes, etc.)
          ???
        }
      } catch {
        case ex: IOException =>
          throw new DiskException(ex)
      }

      stay()

    case Event(AcceptPartitionsCommand(partitions), NoData) =>
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

      goto(WaitingForEventBatch)
  }

  when(WaitingForEventBatch) {
    case Event(EventBatch(cursor, events), NoData) =>
      log.debug(s"Got event batch for cursor $cursor")

      val filteredEvents = events.getOrElse(Nil).filter(e => e.eventClass == "ARTICLE_UPDATE")

      val commands = filteredEvents.map { event =>
        val sku = event.body.get("sku").asText()

        if (sku == null || sku == "") {
          println(event)
          println("Aaaaa")
        }

        assert(sku != null)
        assert(sku != "")

        HashFS.PutCommand(sku, Json.write(event.body))
      }
      hashFS ! HashFS.Commands(commands)
      offsetWriter ! OffsetWriter.WriteOffset(cursor.partition, cursor.offset)
      goto(PersistingEventBatch) using PersistingData(eventsPersisted = false, offsetsPersisted = false, sender())
  }

  when(PersistingEventBatch) {
    case Event(_: EventBatch, _) =>
      stash()
      stay()

    case Event(Ack, PersistingData(false, true, eventBatchSender)) if sender() == hashFS =>
      log.debug("Event batch persisted")
      eventBatchSender ! Ack

      unstashAll()

      goto(WaitingForEventBatch) using NoData

    case Event(Ack, persistingData @ PersistingData(false, false, _)) if sender() == hashFS =>
      stay() using persistingData.copy(eventsPersisted = true)


    case Event(Ack, PersistingData(true, false, eventBatchSender)) if sender() == offsetWriter =>
      log.debug("Offset persisted")
      eventBatchSender ! Ack

      unstashAll()

      goto(WaitingForEventBatch) using NoData

    case Event(Ack, persistingData @ PersistingData(false, false, _)) if sender() == offsetWriter =>
      stay() using persistingData.copy(offsetsPersisted = true)

  }

  whenUnhandled {
    case Event(unexpected, _) =>
      log.error(s"Unexpected message $unexpected in state ${this.stateName} with data ${this.stateData} and sender ${sender()}")
      throw new UnexpectedMessageException(unexpected)
  }


//  def waitingForEventBatch: Receive = {
//    case eventBatch: EventBatch =>
//      eventBatch.events.foreach { events =>
//        events.foreach(persist)
//      }
//      sender() ! Ack
//  }


//  private def persist(event: NakadiEvent): Unit = {
////    event.stateData match {
////      case "ARTICLE_UPDATE" =>
//    //@todo: parametrize id discovery
//    val id = event.data.get("sku").textValue()
//    hashFS forward HashFS.Put(id, Json.write(event))
////    }
//  }
}

object DiskPersistor {
  sealed trait State
  private case object Initialization extends State
  private case object WaitingForEventBatch extends State
  private case object PersistingEventBatch extends State

  sealed trait Data
  private case object NoData extends Data
  private final case class PersistingData(eventsPersisted: Boolean, offsetsPersisted: Boolean, eventBatchSender: ActorRef) extends Data

  // Commands and theirs results

  case object InitCommand

  final case class AcceptPartitionsCommand(partitions: List[Partition])
  final case class PartitionAndLastOffset(partition: String, lastOffset: Option[String])
  final case class PartitionsAccepted(partitionAndLastOffsetList: List[PartitionAndLastOffset])
}
