/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.nakadi

import akka.actor.{Actor, ActorLogging, Props}
import akka.pattern.AskTimeoutException
import org.zalando.znap.config.{Config, DynamoDBDestination, SnapshotTarget}
import org.zalando.znap.nakadi.GetPartitionsWorker.Partitions
import org.zalando.znap.persistence.PersistorCommands
import org.zalando.znap.persistence.dynamo.DynamoPersistor
import org.zalando.znap.utils.{EscalateEverythingSupervisorStrategy, NoUnexpectedMessages}

import scala.concurrent.duration.FiniteDuration

/**
  * Root snapshotter for a Nakadi target.
  */
class NakadiTargetSnapshotter(snapshotTarget: SnapshotTarget,
                              config: Config,
                              tokens: NakadiTokens) extends Actor
    with NoUnexpectedMessages with ActorLogging {

  override val supervisorStrategy = new EscalateEverythingSupervisorStrategy

  import NakadiTargetSnapshotter._
  import akka.pattern.{ask, pipe}
  import context.dispatcher
  import org.zalando.znap.utils._

  private val persistor = snapshotTarget.destination match {
    case dynamoDBDestination: DynamoDBDestination => context.actorOf(Props(
      classOf[DynamoPersistor], snapshotTarget, config
    ))
  }

  override def preStart(): Unit = {
    log.info(s"Starting snapshotter for target ${snapshotTarget.id}")

    implicit val timeout = config.DefaultAskTimeout
    val getPartitionsWorker = context.actorOf(
      Props(classOf[GetPartitionsWorker], snapshotTarget.source, config, tokens),
      s"GetPartitionsWorker-${ActorNames.randomPart()}")
    val f = getPartitionsWorker ? GetPartitionsWorker.GetPartitionsCommand
    f.pipeTo(self)

    persistor ! PersistorCommands.Init
  }

  def initialization: Receive = {
    case Partitions(partitions) =>
      log.info(s"Got partitions for Nakadi target ${snapshotTarget.id}: $partitions")
      persistor ! PersistorCommands.AcceptPartitions(partitions)

      context.system.scheduler.scheduleOnce(
        config.Persistence.Disk.SnapshotInitTimeout,
        self,
        PersistorAcceptPartitionsTimeout(config.Persistence.Disk.SnapshotInitTimeout))

    case PersistorCommands.PartitionsAccepted(partitionAndLastOffsetList) =>
      partitionAndLastOffsetList.foreach { partitionAndLastOffset =>
        context.actorOf(
          Props(classOf[NakadiReader],
            partitionAndLastOffset.partition,
            // TODO: make initial offset configurable (e.g. restart from start)
            partitionAndLastOffset.lastOffset,
            snapshotTarget.source, config, tokens,
            persistor),
          s"NakadiReader-${snapshotTarget.id}-${partitionAndLastOffset.partition}-${partitionAndLastOffset.lastOffset.getOrElse("BEGIN")}-${ActorNames.randomPart()}"
        )
      }
      context.become(working)

    case scala.util.Failure(ex: AskTimeoutException) =>
      throw ex

    case PersistorAcceptPartitionsTimeout(t) =>
      throw new TimeoutException(s"Disk persistor initialization timeout ($t) for target ${snapshotTarget.id}.")
  }

  def working: Receive = {
    case PersistorAcceptPartitionsTimeout(t) =>
      ignore()
  }

  override def receive: Receive = initialization
}

object NakadiTargetSnapshotter {
  trait LocalTimeout
  final case class PersistorAcceptPartitionsTimeout(timeout: FiniteDuration) extends LocalTimeout
}
