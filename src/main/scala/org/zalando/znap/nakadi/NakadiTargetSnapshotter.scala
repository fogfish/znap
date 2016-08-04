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
class NakadiTargetSnapshotterSup(
  snapshotTarget: SnapshotTarget,
  config: Config,
  tokens: NakadiTokens) extends Supervisor {

  override
  val supervisorStrategy = strategyOneForOne(5, 300 seconds)

  def init = Seq(writer(), reader(), service(), leader())

  def writer() =
    snapshotTarget.destination match {
      case dynamoDBDestination: DynamoDBDestination =>
        Supervisor.Worker("writer",
          Props(classOf[DynamoPersistor], snapshotTarget, config))
    }

  def reader() =
    Supervisor.Supervisor("reader",
      Props(classOf[NakadiTargetSnapshotReader], snapshotTarget, config))

  def service() =
    Supervisor.Worker("entity", SnapshotEntityService.spec(context.actorSelection("reader/dynamodb")))

  def leader() =
    Supervisor.Worker("leader",
      Props(classOf[NakadiTargetSnapshotter], snapshotTarget, config, tokens))
}


class NakadiTargetSnapshotter(snapshotTarget: SnapshotTarget,
                              config: Config,
                              tokens: NakadiTokens) extends Actor
    with NoUnexpectedMessages with ActorLogging {

  override val supervisorStrategy = new EscalateEverythingSupervisorStrategy

  import NakadiTargetSnapshotter._
  import akka.pattern.{ask, pipe}
  import context.dispatcher
  import org.zalando.znap.utils._

  private val persistor = Supervisor.child(context.parent, "writer")(context.system).get


  override def preStart(): Unit = {
    log.info(s"Starting snapshotter for target ${snapshotTarget.id}")

    implicit val timeout = config.DefaultAskTimeout

    val pool = snapshotTarget.source.asInstanceOf[NakadiSource].uri.getAuthority
    val f = QueueService.pool(pool)(context.system) ? GetPartitionsWorker.GetPartitionsCommand
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


class NakadiTargetSnapshotReader(snapshotTarget: SnapshotTarget, config: Config) extends Supervisor {
  override
  def supervisorStrategy = strategyFailSafe()

  def init = Seq(dynamodb())

  def dynamodb() =
    snapshotTarget.destination match {
      case dynamoDBDestination: DynamoDBDestination =>
        Supervisor.Worker("dynamodb", Props(classOf[DynamoDBEntityReader], snapshotTarget, config))
    }
}
