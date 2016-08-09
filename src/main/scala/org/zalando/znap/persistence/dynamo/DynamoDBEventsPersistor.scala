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
import org.zalando.znap.nakadi.objects.NakadiEvent
import org.zalando.znap.persistence.EventsPersistor
import org.zalando.znap.utils.{Compressor, Json}

import scala.concurrent.{ExecutionContext, Future}

class DynamoDBEventsPersistor(snapshotTarget: SnapshotTarget,
                              override protected val dynamoDB: DynamoDB)
                             (executionContext: ExecutionContext) extends EventsPersistor with DynamoDBWriter {
  private val logger = LoggerFactory.getLogger(classOf[DynamoDBEventsPersistor])

  private val dynamoDBDestination: DynamoDBDestination = snapshotTarget.destination.asInstanceOf[DynamoDBDestination]

  private implicit val ec = executionContext

  override def persist(events: List[NakadiEvent]): Future[Unit] = Future {
    if (events.nonEmpty) {
      events.grouped(Config.DynamoDB.Batches.WriteBatchSize)
        .foreach(writeEventGroup)
    }
  }

  private def writeEventGroup(events: List[NakadiEvent]): Unit = {
    val updateItems = new TableWriteItems(dynamoDBDestination.tableName)

    events.foreach { e =>
      val key = snapshotTarget.key.foldLeft(e.body) { case (agg, k) =>
        agg.get(k)
      }.asText()

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
