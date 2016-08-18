/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.restapi

import akka.actor.{Actor, ActorLogging, Props}
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import org.zalando.znap.config.{Config, DynamoDBDestination, SnapshotTarget}
import org.zalando.znap.service.EntityReaderService
import org.zalando.znap.utils.{Compressor, NoUnexpectedMessages}

class DynamoDBEntityReader(snapshotTarget: SnapshotTarget) extends Actor with NoUnexpectedMessages with ActorLogging {
  import EntityReaderService._

  private val dynamoDBDestination = snapshotTarget.destination.asInstanceOf[DynamoDBDestination]

  private val client = new AmazonDynamoDBClient()
  client.withEndpoint(dynamoDBDestination.uri.toString)

  private val dynamoDB = new DynamoDB(client)

  private val table = dynamoDB.getTable(dynamoDBDestination.tableName)

  override def receive: Receive = {
    case GetEntityCommand(key) =>
      val item = table.getItem(Config.DynamoDB.KVTables.Attributes.Key, key)
      if (item == null) {
        sender() ! Entity(key, None)
      } else {
        // Work with values based on theirs types.
        // String - not compressed value, Array[Byte] (binary) - compressed.
        val valueClass = item.getTypeOf(Config.DynamoDB.KVTables.Attributes.Value)
        val value =
          if (valueClass == classOf[String]) {
            item.getString(Config.DynamoDB.KVTables.Attributes.Value)
          } else if (valueClass == classOf[Array[Byte]]) {
            val compressed = item.getBinary(Config.DynamoDB.KVTables.Attributes.Value)
            Compressor.decompress(compressed)
          } else {
            throw new Exception(s"Invalid type of value: ${valueClass.getName}")
          }

        sender() ! Entity(key, Some(value))
      }
  }
}

object DynamoDBEntityReader {
  def props(snapshotTarget: SnapshotTarget): Props = {
    Props(classOf[DynamoDBEntityReader], snapshotTarget)
  }
}