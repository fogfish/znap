/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.metrics

import akka.actor.{Actor, ActorLogging, Props}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.FiniteDuration

class MetricsCollector extends Actor with ActorLogging {
  import MetricsCollector._

  private val tickInterval = FiniteDuration(10, scala.concurrent.duration.SECONDS)

  private val getEntityLatencies = ArrayBuffer.empty[Long]
  private val getEntityFromDynamoLatencies = ArrayBuffer.empty[Long]

  override def preStart(): Unit = {
    implicit val ec = context.system.dispatcher
    context.system.scheduler.schedule(
      tickInterval, tickInterval, self, Tick
    )
  }

  override def receive: Receive = {
    case GetEntityLatency(latency) =>
//      println(s"Get $latency")
      getEntityLatencies.append(latency)

    case GetEntityFromDynamoLatency(latency) =>
//      println(s"Dyn $latency")
      getEntityFromDynamoLatencies.append(latency)

    case Tick =>
      printToLog("Get entity latency (REST)", getEntityLatencies)
      getEntityLatencies.clear()

      printToLog("Get entity latency (DynamoDB)", getEntityFromDynamoLatencies)
      getEntityFromDynamoLatencies.clear()
  }

  private def printToLog(name: String, metrics: ArrayBuffer[Long]): Unit = {
    if (metrics.nonEmpty) {
      val latenciesSorted = metrics.sorted
      val entityLatencyMin = latenciesSorted.head
      val entityLatencyMax = latenciesSorted.last
      val entityLatencySum = latenciesSorted.sum
      val p95 = (latenciesSorted.length * 0.95).toInt
      val p95Str =
        if (p95 >= 0 && p95 < metrics.length) {
          s", 95% <= ${metrics(p95)}"
        } else {
          ""
        }

      log.info(s"$name in last $tickInterval: " +
        s"avg. ${entityLatencySum.toDouble / metrics.length}, " +
        s"max. $entityLatencyMax, " +
        s"min. $entityLatencyMin$p95Str")
    }
  }
}

object MetricsCollector {
  val name = "metrics"

  def props(): Props = {
    Props(classOf[MetricsCollector])
  }

  private case object Tick

  final case class GetEntityLatency(latency: Long)
  final case class GetEntityFromDynamoLatency(latency: Long)
}
