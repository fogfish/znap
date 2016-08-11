/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.persistence.dynamo

import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec
import org.zalando.znap.config.{Config, DynamoDBDestination, SnapshotTarget}
import org.zalando.znap.persistence.OffsetReader

import scala.concurrent.{ExecutionContext, Future}

class DynamoDBOffsetReader(snapshotTarget: SnapshotTarget,
                           dynamoDB: DynamoDB)
                          (executionContext: ExecutionContext) extends OffsetReader {
  import collection.JavaConverters._

  private val dynamoDBDestination = snapshotTarget.destination.asInstanceOf[DynamoDBDestination]

  override def getLastOffsets: Future[Map[String, String]] = {
    implicit val ec = executionContext
    Future {
      val offsetsTable = dynamoDB.getTable(dynamoDBDestination.offsetsTableName)
      val items = offsetsTable.scan(new ScanSpec())
      val offsetMap = items.asScala.map { item =>
        val partition = item.getString(Config.DynamoDB.KVTables.Attributes.Key)
        val offset = item.getString(Config.DynamoDB.KVTables.Attributes.Value)
        partition -> offset
      }.toMap
      offsetMap
    }
  }
}