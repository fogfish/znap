/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.dumps

import akka.actor.ActorRef
import org.zalando.znap.config.SnapshotTarget

class DumpTracker {
  private var dumpUids = Map.empty[SnapshotTarget, DumpUID]
  private var dumpUidsReverse = Map.empty[DumpUID, SnapshotTarget]
  private var dumpActors = Map.empty[DumpUID, ActorRef]
  private var dumpActorsReverse = Map.empty[ActorRef, DumpUID]
  private var dumpStatuses = Map.empty[DumpUID, DumpStatus]

  def getRunningDumps(): List[DumpUID] = {
    dumpStatuses.toList.collect {
      case (dumpUid, status) if status == DumpRunning =>
        dumpUid
    }
  }

  def getStatus(dumpUID: DumpUID): DumpStatus = {
    dumpStatuses.getOrElse(dumpUID, UnknownDump)
  }

  def dumpStarted(target: SnapshotTarget, dumpUID: DumpUID, dumpRunner: ActorRef): Unit = {
    if (dumpUids.contains(target)) {
      throw new IllegalStateException(s"A dump for this target is already running")
    } else if (dumpActorsReverse.contains(dumpRunner)) {
      throw new IllegalStateException(s"A dump with this runner is already running")
    } else if (dumpUidsReverse.contains(dumpUID)) {
      throw new IllegalStateException(s"A dump with this uid is already running")
    } else if (dumpStatuses.contains(dumpUID)) {
      throw new IllegalStateException(s"Can't start a dump with the uid of some previous dump")
    } else {
      assert(!dumpActors.contains(dumpUID))

      dumpUids += target -> dumpUID
      dumpUidsReverse += dumpUID -> target
      dumpActors += dumpUID -> dumpRunner
      dumpActorsReverse += dumpRunner -> dumpUID
      dumpStatuses += dumpUID -> DumpRunning
    }
  }

  def getDumpUidIfRunning(pipeline: SnapshotTarget): Option[DumpUID] = {
    dumpUids.get(pipeline)
  }

  def getRunner(dumpUID: DumpUID): ActorRef = {
    dumpActors(dumpUID)
  }

  def dumpFinishedSuccessfully(dumpRunner: ActorRef): DumpUID = {
    finishWithStatus(dumpRunner, DumpFinishedSuccefully)
  }

  def dumpAborted(dumpRunner: ActorRef): DumpUID = {
    finishWithStatus(dumpRunner, DumpAborted)
  }

  def dumpFailed(dumpRunner: ActorRef, message: String): DumpUID = {
    finishWithStatus(dumpRunner, DumpFailed(message))
  }

  def finishWithStatus(dumpRunner: ActorRef, status: DumpStatus): DumpUID = {
    dumpActorsReverse.get(dumpRunner) match {
      case Some(uid) =>
        val pipeline = dumpUidsReverse(uid)

        dumpUids -= pipeline
        dumpUidsReverse -= uid
        dumpActors -= uid
        dumpActorsReverse -= dumpRunner
        dumpStatuses += uid -> status
        uid

      case _ =>
        throw new IllegalStateException(s"Unknown runner $dumpRunner")
    }
  }
}
