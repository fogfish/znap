/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.dumps

import java.util.UUID

import akka.actor.SupervisorStrategy.{Escalate, Stop}
import akka.actor.{Actor, ActorLogging, OneForOneStrategy, Props, SupervisorStrategy}
import org.zalando.znap.config.{SnapshotTarget, SqsDumping}
import org.zalando.znap.source.nakadi.NakadiTokens
import org.zalando.znap.utils.{NoUnexpectedMessages, ThrowableUtils}

import scala.util.control.NonFatal

class DumpManager(tokens: NakadiTokens) extends Actor with NoUnexpectedMessages with ActorLogging {
  import DumpManager._

  private val dumpTracker = new DumpTracker

  // TODO remove finished dumps by timer

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case NonFatal(ex) =>
      val dumpUid = dumpTracker.dumpFailed(sender(), ex.getMessage)
      log.info(s"Dump $dumpUid failed with ${ThrowableUtils.getStackTraceString(ex)}")
      Stop

    case _ => Escalate
  }

  override def receive: Receive = {
    case GetDumpsCommand =>
      sender() ! getDumps()

    case DumpCommand(target, forceRestart) =>
      val result = startDump(target, forceRestart)
      log.info(s"Received dump command for target ${target.id}, result: $result")
      sender() ! result

    case GetDumpStatus(dumpUid) =>
      val result = getDumpStatus(dumpUid)
      sender() ! result

    case AbortDump(dumpUid) =>
      val result = abortDump(dumpUid)
      sender() ! result

    case SqsDumpRunner.Finished =>
      val dumpUid = dumpTracker.dumpFinishedSuccessfully(sender())
      log.info(s"Dump $dumpUid finished successfully")
  }

  private def getDumps(): GetDumpsCommandResult = {
    GetDumpsCommandResult(dumpTracker.getRunningDumps())
  }

  private def startDump(target: SnapshotTarget,
                        forceRestart: Boolean): DumpCommandResult = {
    dumpTracker.getDumpUidIfRunning(target) match {
      case Some(dumpUID) if !forceRestart =>
        AnotherDumpAlreadyRunning(dumpUID)

      case Some(oldDumpUid) if forceRestart =>
        val oldDumpRunner = dumpTracker.getRunner(oldDumpUid)
        oldDumpRunner ! SqsDumpRunner.AbortDump

        val oldDumpUid1 = dumpTracker.dumpAborted(oldDumpRunner)
        assert(oldDumpUid == oldDumpUid1)
        log.info(s"Dump $oldDumpUid aborted")

        startNewDump(target)

      case None =>
        startNewDump(target)
    }
  }

  private def startNewDump(target: SnapshotTarget): DumpCommandResult = {
    val uid = s"${target.id}-${UUID.randomUUID().toString}"

    val dumpRunnerProps = target.dumping.map {
      case _: SqsDumping =>
        Props(classOf[SqsDumpRunner], tokens, target)
    }

    dumpRunnerProps match {
      case Some(p) =>
        val dumpRunner = context.actorOf(p)
        dumpTracker.dumpStarted(target, uid, dumpRunner)
        DumpStarted(uid)

      case None =>
        DumpingNotConfigured
    }
  }

  private def getDumpStatus(dumpUID: DumpUID): DumpStatus = {
    dumpTracker.getStatus(dumpUID)
  }

  private def abortDump(dumpUID: DumpUID): DumpStatus = {
    val oldDumpRunner = dumpTracker.getRunner(dumpUID)
    oldDumpRunner ! SqsDumpRunner.AbortDump

    val dumpUID1 = dumpTracker.dumpAborted(oldDumpRunner)
    assert(dumpUID == dumpUID1)
    log.info(s"Dump $dumpUID aborted")

    dumpTracker.getStatus(dumpUID)
  }
}

object DumpManager {

  val name = "dump-manager"

  case object GetDumpsCommand
  final case class GetDumpsCommandResult(dumpUids: List[DumpUID])

  final case class DumpCommand(snapshotTarget: SnapshotTarget,
                               forceRestart: Boolean)
  sealed trait DumpCommandResult
  final case class DumpStarted(dumpUid: DumpUID) extends DumpCommandResult
  final case class AnotherDumpAlreadyRunning(dumpUid: DumpUID) extends DumpCommandResult
  case object DumpingNotConfigured extends DumpCommandResult

  final case class GetDumpStatus(dumpUid: DumpUID)
  final case class AbortDump(dumpUid: DumpUID)


  def props(tokens: NakadiTokens): Props = {
    Props(classOf[DumpManager], tokens)
  }
}