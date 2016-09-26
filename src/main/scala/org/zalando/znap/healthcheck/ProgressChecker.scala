/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.healthcheck

import akka.actor.{Actor, ActorLogging, Props}
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import org.zalando.znap.config.{Config, DynamoDBDestination, NakadiSource}
import org.zalando.znap.persistence.dynamo.DynamoDBOffsetReader
import org.zalando.znap.source.nakadi.{NakadiPartitionGetter, NakadiTokens}
import org.zalando.znap.utils.{NoUnexpectedMessages, ThrowableUtils}

import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

class ProgressChecker(tokens: NakadiTokens) extends Actor with NoUnexpectedMessages with ActorLogging {
  import ProgressChecker._

  private val checkActions = Config.Targets.map { target =>
    val getPartitionsFunc = target.source match {
      case nakadiSource: NakadiSource =>
        val partitionGetter = new NakadiPartitionGetter(nakadiSource, tokens)(context.system)
        partitionGetter.getPartitions _

      case s =>
        throw new Exception(s"Source type $s is not supported")
    }

    val getOffsetFunc = target.destination match {
      case dynamoDBDestination: DynamoDBDestination =>
        val dynamoDBClient = new AmazonDynamoDBClient()
        dynamoDBClient.withEndpoint(dynamoDBDestination.uri.toString)
        val dynamoDB = new DynamoDB(dynamoDBClient)

        val dispatcher = context.system.dispatchers.lookup(Config.Akka.DynamoDBDispatcher)
        val offsetReader = new DynamoDBOffsetReader(target, dynamoDB)(dispatcher)
        offsetReader.init()

        offsetReader.getLastOffsets _

      case d =>
        throw new Exception(s"Destination type $d is not supported")
    }

    (target.id, getPartitionsFunc, getOffsetFunc)
  }

  private val tickInterval = FiniteDuration(1, scala.concurrent.duration.SECONDS)

  override def preStart(): Unit = {
    scheduleTick()
  }

  private def scheduleTick(): Unit = {
    implicit val ec = context.dispatcher
    context.system.scheduler.scheduleOnce(tickInterval, self, Tick)
  }

  private val progressBarSize = 50

  override def receive: Receive = {
    case Tick =>
      import scala.concurrent.duration._
      checkActions.foreach { case (id, getPartitionsFunc, getOffsetFunc) =>
        try {
          val partitionsF = getPartitionsFunc()
          val offsetsF = getOffsetFunc()
          val partitions = Await.result(partitionsF, 30.seconds)
          val offsets = Await.result(offsetsF, 30.seconds)

          partitions.sortBy(_.partition).foreach { partition =>
            val start = partition.oldestAvailableOffset.toLong
            val end = partition.newestAvailableOffset.toLong

            print(s"$id ${partition.partition}")

            offsets.get(partition.partition) match {
              case Some(offset) =>
                val offsetNum = offset.toLong

                if (offsetNum >= start && offsetNum <= end) {
                  val length = end - start
                  val progress = offsetNum - start
                  val positionRaw = ((progressBarSize * progress.toDouble) / length).toInt
                  val position =
                    if (positionRaw < 0) {
                      0
                    } else if (positionRaw > (progressBarSize - 1)) {
                      progressBarSize - 1
                    } else {
                      positionRaw
                    }

                  val msg = "[" +
                    "." * position +
                    "*" +
                    ("." * (progressBarSize - position - 1)) +
                    s"] $start-_${offsetNum}_-$end"
                  log.info(msg)
                } else {
                  log.error(s"In $id, the current offset is $offsetNum, available offsets are $start - $end.")
                }
              case _ =>
                val msg = s"[..................................................] $start-__-$end"
                log.info(msg)
            }
          }
        } catch {
          case NonFatal(e) =>
            log.warning(s"Can't print progress for $id: ${ThrowableUtils.getStackTraceString(e)}")
        }
      }

      scheduleTick()
  }
}

object ProgressChecker {
  val name = "progress-checker"

  private case object Tick

  def props(tokens: NakadiTokens): Props = {
    Props(classOf[ProgressChecker], tokens)
  }
}
