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
import org.zalando.znap.config.{Config, DynamoDBDestination, SnapshotTarget}
import org.zalando.znap.nakadi.objects.{Cursor, EventBatch, NakadiEvent}
import org.zalando.znap.persistence.dynamo.WriteEventBatchChannel.{BatchWritten, WriteBatchCommand}
import org.zalando.znap.utils.{Compressor, NoUnexpectedMessages}

class WriteEventBatchChannel(snapshotTarget: SnapshotTarget,
                             dynamoDB: DynamoDB) extends Actor with NoUnexpectedMessages with ActorLogging {

  private val dynamoDBDestination = snapshotTarget.destination.asInstanceOf[DynamoDBDestination]

  override def preStart(): Unit = {
    log.debug(s"WriteEventBatchChannel for target ${snapshotTarget.id} started. Dispatcher ${context.dispatcher}")
  }

  override def receive: Receive = {
    case WriteBatchCommand(EventBatch(cursor, events)) =>
      writeEvents(events.getOrElse(Nil))
      writeOffsets(cursor)
      sender() ! BatchWritten
  }

  private def writeEvents(events: List[NakadiEvent]): Unit = {
    events.grouped(Config.DynamoDB.Batches.WriteBatchSize)
      .foreach(writeEventGroup)
  }

  private def writeEventGroup(events: List[NakadiEvent]): Unit = {
    val updateItems = new TableWriteItems(dynamoDBDestination.tableName)

    events.foreach { e =>
      val key = snapshotTarget.key.foldLeft(e.body) { case (agg, k) =>
        agg.get(k)
      }.asText()

      val item = new Item()
        .withPrimaryKey(Config.DynamoDB.KVTables.Attributes.Key, key)

      if (snapshotTarget.compress) {
        item.withBinary(Config.DynamoDB.KVTables.Attributes.Value, Compressor.compress(e.body.toString))
      } else {
        item.withString(Config.DynamoDB.KVTables.Attributes.Value, e.body.toString)
      }

      updateItems.addItemToPut(item)
    }

    if (updateItems.getItemsToPut.size() > 0) {
      writeWithRetries(updateItems)
    }
  }

  private def writeOffsets(cursor: Cursor): Unit = {
    val offsetUpdateItems = new TableWriteItems(dynamoDBDestination.offsetsTableName)

    offsetUpdateItems.addItemToPut(new Item()
        .withPrimaryKey(Config.DynamoDB.KVTables.Attributes.Key, cursor.partition)
        .withString(Config.DynamoDB.KVTables.Attributes.Value, cursor.offset)
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
