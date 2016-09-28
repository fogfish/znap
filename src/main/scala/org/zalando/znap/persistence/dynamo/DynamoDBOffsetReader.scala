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
import org.zalando.znap.config.{Config, DynamoDBOffsetPersistence}
import org.zalando.znap.persistence.OffsetReaderSync

import scala.concurrent.{ExecutionContext, Future}

class DynamoDBOffsetReader(dynamoDBOffsetPersistence: DynamoDBOffsetPersistence,
                           dynamoDB: DynamoDB)
                          (executionContext: ExecutionContext) extends OffsetReaderSync {
  import collection.JavaConverters._

  private implicit val ec = executionContext

  override def init(): Unit = {}

  override def getLastOffsets: Future[Map[String, String]] = Future {
    val offsetsTable = dynamoDB.getTable(dynamoDBOffsetPersistence.tableName)
    val items = offsetsTable.scan(new ScanSpec())
    val offsetMap = items.asScala.map { item =>
      val partition = item.getString(Config.DynamoDB.KVTables.Attributes.Key)
      val offset = item.getString(Config.DynamoDB.KVTables.Attributes.Value)
      partition -> offset
    }.toMap
    offsetMap
  }
}
