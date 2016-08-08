/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.nakadi

import akka.actor._
import akka.pattern.AskTimeoutException
import org.zalando.scarl.Supervisor.Specs
import org.zalando.scarl._
import org.zalando.znap.config.{NakadiSource, Config, DynamoDBDestination, SnapshotTarget}
import org.zalando.znap.nakadi.GetPartitionsWorker.Partitions
import org.zalando.znap.persistence.PersistorCommands
import org.zalando.znap.persistence.dynamo.DynamoPersistor
import org.zalando.znap.service.queue.QueueService
import org.zalando.znap.service.{SnapshotEntityService, DynamoDBEntityReader}
import org.zalando.znap.utils.{EscalateEverythingSupervisorStrategy, NoUnexpectedMessages}
import scala.concurrent.duration._

/**
  * snapshot management subsystem
  *
  */
class NakadiTargetSnapshotterRoot(snapshot: SnapshotTarget, tokens: NakadiTokens) extends Supervisor {

  override
  val supervisorStrategy = strategyOneForOne(5, 30.seconds)

  private val sink = actorOf( Props(classOf[DynamoPersistor], snapshot) )

  private val reader = actorOf( Props(classOf[NakadiTargetSnapshotReader], snapshot), "reader" )

  private val leader = actorOf( Props(classOf[NakadiTargetSnapshotter], snapshot, sink, tokens) )

  def specs = Seq(
    SnapshotEntityService.spec(context.actorSelection("reader/dynamodb"))
  )
}


class NakadiTargetSnapshotter(
  snapshotTarget: SnapshotTarget,
  persistor: ActorRef,
  tokens: NakadiTokens
) extends Actor with NoUnexpectedMessages with ActorLogging {

  override val supervisorStrategy = new EscalateEverythingSupervisorStrategy

  import NakadiTargetSnapshotter._
  import akka.pattern._
  import context.dispatcher
  import org.zalando.znap.utils._


  override def preStart(): Unit = {
    log.info(s"Starting snapshotter for target ${snapshotTarget.id}")

    implicit val timeout = Config.DefaultAskTimeout

    val pool = snapshotTarget.source.asInstanceOf[NakadiSource].uri
    QueueService.pool(pool)(context.system) map {_ ? GetPartitionsWorker.GetPartitionsCommand pipeTo self}

    persistor ! PersistorCommands.Init
  }

  def initialization: Receive = {
    case Partitions(partitions) =>
      log.info(s"Got partitions for Nakadi target ${snapshotTarget.id}: $partitions")
      persistor ! PersistorCommands.AcceptPartitions(partitions)

      context.system.scheduler.scheduleOnce(
        Config.Persistence.Disk.SnapshotInitTimeout,
        self,
        PersistorAcceptPartitionsTimeout(Config.Persistence.Disk.SnapshotInitTimeout))

    case PersistorCommands.PartitionsAccepted(partitionAndLastOffsetList) =>
      partitionAndLastOffsetList.foreach { partitionAndLastOffset =>
        context.actorOf(
          Props(classOf[NakadiReader],
            partitionAndLastOffset.partition,
            // TODO: make initial offset configurable (e.g. restart from start)
            partitionAndLastOffset.lastOffset,
            snapshotTarget.source, tokens,
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


class NakadiTargetSnapshotReader(snapshotTarget: SnapshotTarget) extends Supervisor {
  override
  def supervisorStrategy = strategyFailSafe()

  def specs = Seq(
    Specs("dynamodb", Props(classOf[DynamoDBEntityReader], snapshotTarget))
  )
}
