/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.healthcheck

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import org.slf4j.LoggerFactory
import org.zalando.znap.config._
import org.zalando.znap.metrics.Instrumented
import org.zalando.znap.persistence.dynamo.DynamoDBOffsetReader
import org.zalando.znap.source.nakadi.{NakadiPartitionGetter, NakadiTokens}
import org.zalando.znap.utils.{NoUnexpectedMessages, ThrowableUtils}

import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

class ProgressChecker(tokens: NakadiTokens) extends Actor with NoUnexpectedMessages with ActorLogging with Instrumented {
  import ProgressChecker._

  private val progressHolders = Config.Targets.map { target =>
    new ProgressHolder(target, tokens)(context.system)
  }

  override def preStart(): Unit = {
    progressHolders.foreach(_.registerProgress())
    scheduleTick()
  }

  private def scheduleTick(): Unit = {
    val tickInterval = FiniteDuration(1, scala.concurrent.duration.MINUTES)
    implicit val ec = context.dispatcher
    context.system.scheduler.scheduleOnce(tickInterval, self, Tick)
  }

  override def receive: Receive = {
    case Tick =>
      progressHolders.foreach(_.registerProgress())
      scheduleTick()
  }
}

object ProgressChecker {
  val name = "progress-checker"

  private case object Tick

  def props(tokens: NakadiTokens): Props = {
    Props(classOf[ProgressChecker], tokens)
  }


  private class ProgressHolder(target: SnapshotTarget,
                               tokens: NakadiTokens)
                              (actorSystem: ActorSystem) extends Instrumented {
    private val logger = LoggerFactory.getLogger(classOf[ProgressHolder])

    import scala.concurrent.duration._

    private val awaitDuration = 30.seconds
    private val progressBarSize = 50

    @volatile private var lastValuesPerPartition_OldestAvailableOffset = Map.empty[String, Long]
    @volatile private var lastValuesPerPartition_NewestAvailableOffset = Map.empty[String, Long]
    @volatile private var lastValuesPerPartition_CurrentOffset = Map.empty[String, Long]

    private val getPartitionsFunc = target.source match {
      case nakadiSource: NakadiSource =>
        val partitionGetter = new NakadiPartitionGetter(nakadiSource, tokens)(actorSystem)
        partitionGetter.getPartitions _

      case s =>
        throw new Exception(s"Source type $s is not supported")
    }

    private val getOffsetFunc = target.offsetPersistence match {
      case dynamoDBOffsetPersistence: DynamoDBOffsetPersistence =>
        val dynamoDBClient = new AmazonDynamoDBClient()
        dynamoDBClient.withEndpoint(dynamoDBOffsetPersistence.uri.toString)
        val dynamoDB = new DynamoDB(dynamoDBClient)

        val dispatcher = actorSystem.dispatchers.lookup(Config.Akka.DynamoDBDispatcher)
        val offsetReader = new DynamoDBOffsetReader(dynamoDBOffsetPersistence, dynamoDB)(dispatcher)
        offsetReader.init()

        offsetReader.getLastOffsets _
    }

    def registerProgress(): Unit = {
      try {
        val partitionsF = getPartitionsFunc()
        val offsetsF = getOffsetFunc()
        val partitions = Await.result(partitionsF, 30.seconds)
        val offsets = Await.result(offsetsF, 30.seconds)

        partitions.sortBy(_.partition).foreach { partition =>
          val start = partition.oldestAvailableOffset.toLong
          val end = partition.newestAvailableOffset.toLong

          lastValuesPerPartition_OldestAvailableOffset += partition.partition -> start
          lastValuesPerPartition_NewestAvailableOffset += partition.partition -> end

          offsets.get(partition.partition) match {
            case Some(offset) =>
              val offsetNum = offset.toLong

              lastValuesPerPartition_CurrentOffset += partition.partition -> offsetNum

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

                val msg = s"${target.id} ${partition.partition} [" +
                  "." * position +
                  "*" +
                  ("." * (progressBarSize - position - 1)) +
                  s"] $start - $offsetNum - $end"
                logger.info(msg)
              } else {
                logger.error(s"In ${target.id} ${partition.partition}, the current offset is $offsetNum, available offsets are $start - $end.")
              }
            case _ =>
              val msg = s"${target.id} ${partition.partition} [..................................................] $start - __ - $end"
              logger.info(msg)
          }
        }
      } catch {
        case NonFatal(e) =>
          logger.warn(s"Can't print progress for ${target.id}: ${ThrowableUtils.getStackTraceString(e)}")
      }
    }

    Await.result(getPartitionsFunc(), awaitDuration).sortBy(_.partition).foreach { p =>
      metrics.gauge(s"${target.id}-${p.partition}-oldestAvailableOffset") {
        lastValuesPerPartition_OldestAvailableOffset.getOrElse(p.partition, -1)
      }
      metrics.gauge(s"${target.id}-${p.partition}-newestAvailableOffset") {
        lastValuesPerPartition_NewestAvailableOffset.getOrElse(p.partition, -1)
      }
      metrics.gauge(s"${target.id}-${p.partition}-currentOffset") {
        lastValuesPerPartition_CurrentOffset.getOrElse(p.partition, -1)
      }
    }
  }
}
