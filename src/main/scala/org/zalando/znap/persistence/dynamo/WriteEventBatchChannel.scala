/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.persistence.dynamo

import akka.actor.{Actor, ActorLogging}
import com.amazonaws.services.dynamodbv2.document.{DynamoDB, Item, TableWriteItems}
import org.zalando.znap.config.{Config, SnapshotTarget}
import org.zalando.znap.nakadi.objects.{Cursor, EventBatch, NakadiEvent}
import org.zalando.znap.persistence.dynamo.WriteEventBatchChannel.{BatchWritten, WriteBatchCommand}
import org.zalando.znap.utils.NoUnexpectedMessages

class WriteEventBatchChannel(target: SnapshotTarget,
                             dynamoDB: DynamoDB,
                             config: Config) extends Actor with NoUnexpectedMessages with ActorLogging {

  override def preStart(): Unit = {
    log.debug(s"WriteEventBatchChannel for target ${target.id} started. Dispatcher ${context.dispatcher}")
  }

  override def receive: Receive = {
    case WriteBatchCommand(EventBatch(cursor, events)) =>
      writeEvents(events.getOrElse(Nil))
      writeOffsets(cursor)
      sender() ! BatchWritten
  }

  private def writeEvents(events: List[NakadiEvent]): Unit = {
    events.grouped(config.DynamoDB.Batches.WriteBatchSize)
      .foreach(writeEventGroup)
  }

  private def writeEventGroup(events: List[NakadiEvent]): Unit = {
    val updateItems = new TableWriteItems(target.id)

    events.foreach { e =>
      val key = target.key.foldLeft(e.body) { case (agg, k) =>
        agg.get(k)
      }.asText()
      updateItems.addItemToPut(
        new Item()
          .withPrimaryKey(config.DynamoDB.KVTables.Attributes.Key, key)
          .withString(config.DynamoDB.KVTables.Attributes.Value, e.body.toString)
      )
    }

    if (updateItems.getItemsToPut.size() > 0) {
      writeWithRetries(updateItems)
    }
  }

  private def writeOffsets(cursor: Cursor): Unit = {
    val offsetUpdateItems = new TableWriteItems(config.DynamoDB.OffsetsTable.Name)
    offsetUpdateItems.addItemToPut(new Item()
        .withPrimaryKey(
          config.DynamoDB.OffsetsTable.Attributes.TargetId, target.id,
          config.DynamoDB.OffsetsTable.Attributes.Partition, cursor.partition)
        .withString(config.DynamoDB.OffsetsTable.Attributes.Offset, cursor.offset)
    )
    writeWithRetries(offsetUpdateItems)
  }

  private def writeWithRetries(writeItems: TableWriteItems): Unit = {
    var outcome = dynamoDB.batchWriteItem(writeItems)

    do {

      // Check for unprocessed keys which could happen if you exceed provisioned throughput

      val unprocessedItems = outcome.getUnprocessedItems

      if (outcome.getUnprocessedItems.size() == 0) {
//        println("No unprocessed items found")
      } else {
        log.debug("Retrieving the unprocessed items")
        outcome = dynamoDB.batchWriteItemUnprocessed(unprocessedItems)
      }
    } while (outcome.getUnprocessedItems.size() > 0)
  }
}

object WriteEventBatchChannel {
  final case class WriteBatchCommand(eventBatch: EventBatch)
  case object BatchWritten
}