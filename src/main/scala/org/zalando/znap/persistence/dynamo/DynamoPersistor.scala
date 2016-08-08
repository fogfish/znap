/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.persistence.dynamo

import akka.actor.{ActorLogging, ActorRef, FSM, Props, Stash}
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import org.zalando.znap.config.{Config, DynamoDBDestination, SnapshotTarget}
import org.zalando.znap.nakadi.Messages.Ack
import org.zalando.znap.nakadi.objects.EventBatch
import org.zalando.znap.objects.Partition
import org.zalando.znap.persistence.PersistorCommands
import org.zalando.znap.persistence.dynamo.DynamoPersistor._
import org.zalando.znap.utils.{EscalateEverythingSupervisorStrategy, UnexpectedMessageException}

class DynamoPersistor(snapshotTarget: SnapshotTarget) extends FSM[State, Data] with Stash with ActorLogging {

  override val supervisorStrategy = new EscalateEverythingSupervisorStrategy

  private val dynamoDBDestination = snapshotTarget.destination.asInstanceOf[DynamoDBDestination]

  private val client = new AmazonDynamoDBClient()
  client.withEndpoint(dynamoDBDestination.uri.toString)

  private val dynamoDB = new DynamoDB(client)

  private val getOffsetsChannel = context.actorOf(Props(
    classOf[GetOffsetsChannel], snapshotTarget, dynamoDB)
      .withDispatcher(Config.Akka.DynamoDBDispatcher)
  )

  private val writeEventBatchChannel = context.actorOf(Props(
    classOf[WriteEventBatchChannel], snapshotTarget, dynamoDB)
     .withDispatcher(Config.Akka.DynamoDBDispatcher)
  )

  startWith(Initialization, NoData)

  when(Initialization) {
    case Event(PersistorCommands.Init, NoData) =>
      stay()

    case Event(PersistorCommands.AcceptPartitions(partitions), NoData) =>
      getOffsetsChannel ! GetOffsetsChannel.GetOffsetsCommand
      goto(WaitingForPartitions) using AcceptingPartitionsData(sender(), partitions)
  }

  when(WaitingForPartitions) {
    case Event(GetOffsetsChannel.Offsets(partitionsAndOffsets), AcceptingPartitionsData(replyTo, partitions)) =>
      // TODO accept and check partitions

      val partitionAndLastOffsetList = partitions.map { p =>
        val offset = partitionsAndOffsets.get(p.partition)
        PersistorCommands.PartitionAndLastOffset(p.partition, offset)
      }

      replyTo ! PersistorCommands.PartitionsAccepted(partitionAndLastOffsetList)
      goto(WaitingForEventBatch) using NoData
  }

  when(WaitingForEventBatch) {
    case Event(eventBatch: EventBatch, NoData) =>
      log.debug(s"Writing batch ${eventBatch.cursor} for target ${snapshotTarget.id}")
      writeEventBatchChannel ! WriteEventBatchChannel.WriteBatchCommand(eventBatch)
      goto(WritingEventBatch) using WritingEventBatchData(sender())
  }

  when(WritingEventBatch) {
    case Event(_: EventBatch, _) =>
      stash()
      stay()

    case Event(WriteEventBatchChannel.BatchWritten, WritingEventBatchData(eventBatchSender)) =>
      log.debug(s"Batch for target ${snapshotTarget.id} written")
      eventBatchSender ! Ack
      goto(WaitingForEventBatch) using NoData
  }

  onTransition {
    case WritingEventBatch -> WaitingForEventBatch =>
      unstashAll()
  }

  whenUnhandled {
    case Event(unexpected, _) =>
      log.error(s"Unexpected message $unexpected in state ${this.stateName} with data ${this.stateData} from ${sender()}")
      throw new UnexpectedMessageException(unexpected, sender())
  }
}

object DynamoPersistor {
  sealed trait State
  private case object Initialization extends State
  private case object WaitingForPartitions extends State
  private case object WaitingForEventBatch extends State
  private case object WritingEventBatch extends State

  sealed trait Data
  private case object NoData extends Data
  private final case class AcceptingPartitionsData(replyTo: ActorRef, partitions: List[Partition]) extends Data
  private final case class WritingEventBatchData(eventBatchSender: ActorRef) extends Data

  case object InitCommand
}
