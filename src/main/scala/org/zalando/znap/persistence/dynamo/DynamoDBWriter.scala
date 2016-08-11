package org.zalando.znap.persistence.dynamo

import com.amazonaws.services.dynamodbv2.document.{DynamoDB, TableWriteItems}
import org.slf4j.LoggerFactory

trait DynamoDBWriter {
  private val logger = LoggerFactory.getLogger(classOf[DynamoDBWriter])

  protected val dynamoDB: DynamoDB

  protected def writeWithRetries(writeItems: TableWriteItems): Unit = {
    var outcome = dynamoDB.batchWriteItem(writeItems)

    do {
      // Check for unprocessed keys which could happen if you exceed provisioned throughput

      val unprocessedItems = outcome.getUnprocessedItems

      if (outcome.getUnprocessedItems.size() == 0) {
        //        println("No unprocessed items found")
      } else {
        logger.debug("Retrieving the unprocessed items")
        outcome = dynamoDB.batchWriteItemUnprocessed(unprocessedItems)
      }
    } while (outcome.getUnprocessedItems.size() > 0)
  }
}
