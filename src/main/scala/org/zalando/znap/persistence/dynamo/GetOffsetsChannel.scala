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
import org.zalando.znap.config.{Config, SnapshotTarget}
import org.zalando.znap.utils.NoUnexpectedMessages

class GetOffsetsChannel(snapshotTarget: SnapshotTarget,
                        dynamoDB: DynamoDB,
                        config: Config) extends Actor with NoUnexpectedMessages with ActorLogging {

  import GetOffsetsChannel._

  import collection.JavaConverters._

  override def receive: Receive = {
    case GetOffsetsCommand =>
      val offsetsTable = dynamoDB.getTable(config.DynamoDB.OffsetsTable.Name)
      val queryResult = offsetsTable.query(config.DynamoDB.OffsetsTable.Attributes.TargetId, snapshotTarget.id)

      val iterator = queryResult.iterator()
      while (iterator.hasNext) {
        val item = iterator.next()
      }

      val offsetMap = queryResult.asScala.map { item =>
        val partition = item.getString(config.DynamoDB.OffsetsTable.Attributes.Partition)
        val offset = item.getString(config.DynamoDB.OffsetsTable.Attributes.Offset)
        partition -> offset
      }.toMap

      sender() ! Offsets(offsetMap)
  }
}

object GetOffsetsChannel {
  case object GetOffsetsCommand

  final case class Offsets(partitionsAndOffsets: Map[String, String])
}