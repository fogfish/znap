/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.persistence.dynamo

import com.amazonaws.services.dynamodbv2.document.{DynamoDB, Item, TableWriteItems}
import org.slf4j.LoggerFactory
import org.zalando.znap.config.{Config, DynamoDBDestination, SnapshotTarget}
import org.zalando.znap.source.nakadi.objects.NakadiEvent
import org.zalando.znap.persistence.EventsWriterSync
import org.zalando.znap.utils.{Compressor, Json}

import scala.concurrent.{ExecutionContext, Future}

class DynamoDBEventsWriter(snapshotTarget: SnapshotTarget,
                           override protected val dynamoDB: DynamoDB) extends EventsWriterSync with DynamoDBWriter {
  private val logger = LoggerFactory.getLogger(classOf[DynamoDBEventsWriter])

  private val dynamoDBDestination: DynamoDBDestination = snapshotTarget.destination.asInstanceOf[DynamoDBDestination]

  override def init(): Unit = {}

  override def write(events: List[NakadiEvent]): Unit = {
    if (events.nonEmpty) {
      events.grouped(Config.DynamoDB.Batches.WriteBatchSize)
        .foreach(writeEventGroup)
    }
  }

  private def writeEventGroup(events: List[NakadiEvent]): Unit = {
    val updateItems = new TableWriteItems(dynamoDBDestination.tableName)

    events.foreach { e =>
      val key = Json.getKey(snapshotTarget.key, e.body)
      val item = new Item()
        .withPrimaryKey(Config.DynamoDB.KVTables.Attributes.Key, key)

      val jsonString = Json.write(e)
      if (snapshotTarget.compress) {
        item.withBinary(Config.DynamoDB.KVTables.Attributes.Value, Compressor.compress(jsonString))
      } else {
        item.withString(Config.DynamoDB.KVTables.Attributes.Value, jsonString)
      }

      updateItems.addItemToPut(item)
    }

    if (updateItems.getItemsToPut.size() > 0) {
      writeWithRetries(updateItems)
    }
  }
}
