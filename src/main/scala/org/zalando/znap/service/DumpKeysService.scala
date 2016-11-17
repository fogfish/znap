package org.zalando.znap.service

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.util.Timeout
import org.zalando.znap.config.SnapshotTarget
import org.zalando.znap.dumps._
import org.zalando.znap.dumps.DumpManager

import scala.concurrent.Future

object DumpKeysService {
  import scala.concurrent.duration._
  import akka.pattern.ask

  def dump(target: SnapshotTarget, forceRestart: Boolean)
          (implicit actorSystem: ActorSystem): Future[DumpManager.DumpCommandResult] = {
    implicit val ec = actorSystem.dispatcher
    getActor(actorSystem).flatMap { ref =>
      implicit val askTimeout = Timeout(10.seconds)
      ref.ask(DumpManager.DumpCommand(target, forceRestart))
        .mapTo[DumpManager.DumpCommandResult]
    }
  }

  def getDumpStatus(dumpUid: String)
                   (implicit actorSystem: ActorSystem): Future[DumpStatus] = {
    implicit val ec = actorSystem.dispatcher
    getActor(actorSystem).flatMap { ref =>
      implicit val askTimeout = Timeout(10.seconds)
      ref.ask(DumpManager.GetDumpStatus(dumpUid))
        .mapTo[DumpStatus]
    }
  }

  def abortDump(dumpUid: String)
               (implicit actorSystem: ActorSystem): Future[DumpStatus] = {
    implicit val ec = actorSystem.dispatcher
    getActor(actorSystem).flatMap { ref =>
      implicit val askTimeout = Timeout(10.seconds)
      ref.ask(DumpManager.AbortDump(dumpUid))
        .mapTo[DumpStatus]
    }
  }

  private def getActor(actorSystem: ActorSystem): Future[ActorRef] = {
    implicit val resolveTimeout = 10.seconds
    val dumpManagerSelection = actorSystem.actorSelection(s"/user/${DumpManager.name}")
    dumpManagerSelection.resolveOne(resolveTimeout)
  }
}
