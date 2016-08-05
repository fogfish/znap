/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.service

import java.util

import akka.stream.scaladsl.{Merge, Source}
import akka.stream.{ActorAttributes, Attributes}
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.model.{AttributeValue, ScanRequest}
import org.zalando.znap.config.{Config, DynamoDBDestination, SnapshotTarget}


object SnapshotService {
  private val totalSegments = 4
  private val scanItemLimit = 10

  def getSnapshotKeys(target: SnapshotTarget): Source[String, Any] = {
    assert(totalSegments > 0)

    val client = {
      val c = new AmazonDynamoDBClient()
      c.withEndpoint(target.destination.asInstanceOf[DynamoDBDestination].uri.toString)
      c
    }

    val sources = (0 until totalSegments).toVector.map { segment =>
      target.destination match {
        case d: DynamoDBDestination =>
          val iterator = new ScannerIterator(
            client, d.tableName, Config.DynamoDB.KVTables.Attributes.Key,
            scanItemLimit, totalSegments, segment)

          Source.fromIterator(() => iterator)
            .addAttributes(ActorAttributes.dispatcher(Config.Akka.DynamoDBDispatcher))
      }
    }

    val resultStream =
      if (totalSegments == 1) {
        sources.head
      } else {
        Source.combine[String, String](sources(0), sources(1), sources.drop(2): _*)(Merge(_))
      }

    resultStream
      .addAttributes(ActorAttributes.dispatcher(Config.Akka.DynamoDBDispatcher))
      .withAttributes(Attributes.inputBuffer(initial = 8, max = 64))
      .async
  }

  private class ScannerIterator(client: AmazonDynamoDBClient,
                                tableName: String,
                                keyAttributeName: String,
                                scanItemLimit: Int,
                                totalSegments: Int,
                                segment: Int) extends scala.collection.Iterator[String] {
    private var lastEvaluatedKey: java.util.Map[String, AttributeValue] = _

    private var items: java.util.List[java.util.Map[String, AttributeValue]] = _
    private var offset = 0
    private var queryNeeded = true
    private var finished = false

    override def next(): String = {
      if (finished) {
        throw new NoSuchElementException
      }
      val result = items.get(offset).get(keyAttributeName).getS
      offset += 1
      if (offset >= items.size()) {
        queryNeeded = true
      }
      result
    }

    override def hasNext: Boolean = {
      if (finished) {
        assert(queryNeeded)
        false
      } else if (queryNeeded) {
        makeQuery()
      } else {
        true
      }
    }

    private def makeQuery(): Boolean = {
      val placeholder = "#k"
      val expressionAttributeMap = new util.HashMap[String, String]()
      expressionAttributeMap.put(placeholder, keyAttributeName)

      val scanRequest = new ScanRequest()
        .withTableName(tableName)
        .withProjectionExpression(placeholder)
        .withExpressionAttributeNames(expressionAttributeMap)
        .withLimit(scanItemLimit)
        .withExclusiveStartKey(lastEvaluatedKey)
        .withTotalSegments(totalSegments)
        .withSegment(segment)

      val result = client.scan(scanRequest)
      lastEvaluatedKey = result.getLastEvaluatedKey

      if (result.getLastEvaluatedKey == null) {
        finished = true
        false
      } else {
        items = result.getItems
        offset = 0
        queryNeeded = false
        true
      }
    }
  }
}
