/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.persistence.dynamo

import akka.actor.{Actor, ActorLogging}
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.document.spec.{QuerySpec, ScanSpec}
import org.zalando.znap.config.{Config, DynamoDBDestination, SnapshotTarget}
import org.zalando.znap.utils.NoUnexpectedMessages

class GetOffsetsChannel(snapshotTarget: SnapshotTarget,
                        dynamoDB: DynamoDB) extends Actor with NoUnexpectedMessages with ActorLogging {

  private val dynamoDBDestination = snapshotTarget.destination.asInstanceOf[DynamoDBDestination]

  import GetOffsetsChannel._

  import collection.JavaConverters._

  override def receive: Receive = {
    case GetOffsetsCommand =>
      val offsetsTable = dynamoDB.getTable(dynamoDBDestination.offsetsTableName)

      val items = offsetsTable.scan(new ScanSpec())

      val offsetMap = items.asScala.map { item =>
        val partition = item.getString(Config.DynamoDB.KVTables.Attributes.Key)
        val offset = item.getString(Config.DynamoDB.KVTables.Attributes.Value)
        partition -> offset
      }.toMap

      sender() ! Offsets(offsetMap)
  }
}

object GetOffsetsChannel {
  case object GetOffsetsCommand

  final case class Offsets(partitionsAndOffsets: Map[String, String])
}
