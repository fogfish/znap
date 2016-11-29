/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.restapi

import java.util

import akka.actor.{Actor, ActorLogging, Props}
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.document.GetItemOutcome
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import org.zalando.znap.TargetId
import org.zalando.znap.config.{Config, DynamoDBDestination}
import org.zalando.znap.metrics.Instrumented
import org.zalando.znap.service.EntityReaderService
import org.zalando.znap.utils.{Compressor, NoUnexpectedMessages}

class DynamoDBEntityReader(targetId: TargetId,
                           client: AmazonDynamoDBClient,
                           dynamoDBDestination: DynamoDBDestination) extends Actor with NoUnexpectedMessages with ActorLogging with Instrumented {
  import EntityReaderService._

//  private val dynamoDB = new DynamoDB(client)
//  private val table = dynamoDB.getTable(dynamoDBDestination.tableName)

  private val timer = metrics.timer(s"get-entity-dynamo-$targetId")

  override def receive: Receive = {
    case GetEntityCommand(key) =>
      sender() ! get(key)
      context.stop(self)
  }

  private def get(key: String): GetEntityCommandResult = {
    try {
      val attributes = new util.HashMap[String, AttributeValue]()
      attributes.put(Config.DynamoDB.KVTables.Attributes.Key, new AttributeValue(key))
      val consistentRead = true

      val result = timer.time {
        client.getItem(dynamoDBDestination.tableName, attributes, consistentRead)
      }

      val outcome = new GetItemOutcome(result)
      val item = outcome.getItem
//      val item = table.getItem(Config.DynamoDB.KVTables.Attributes.Key, key)

      if (item == null) {
        NoEntityFound(key)
      } else {
        // Work with values based on theirs types.
        // String - not compressed value, Array[Byte] (binary) - compressed.
        val valueClass = item.getTypeOf(Config.DynamoDB.KVTables.Attributes.Value)
          if (valueClass == classOf[String]) {
            val value = item.getString(Config.DynamoDB.KVTables.Attributes.Value)
            PlainEntity(key, value)
          } else if (valueClass == classOf[Array[Byte]]) {
            val compressed = item.getBinary(Config.DynamoDB.KVTables.Attributes.Value)
            GzippedEntity(key, compressed)
          } else {
            throw new Exception(s"Invalid type of value: ${valueClass.getName}")
          }
      }
    } catch {
      case e: com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException =>
        log.warning(s"${e.getMessage}, retrying in ")
        ProvisionedThroughputExceeded
    }
  }
}

object DynamoDBEntityReader {
  def props(targetId: TargetId,
            client: AmazonDynamoDBClient,
            dynamoDBDestination: DynamoDBDestination): Props = {
    Props(classOf[DynamoDBEntityReader], targetId, client, dynamoDBDestination)
  }
}