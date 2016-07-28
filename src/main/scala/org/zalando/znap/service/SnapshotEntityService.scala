package org.zalando.znap.service

import akka.actor.{Actor, Props}


object SnapshotEntityService {
  type Commit = (String) => Unit

  def spec() =
    Props(new SnapshotEntityService())
}


class SnapshotEntityService extends PoolService {
  override def props: Props = Props(new SnapshotEntityRequest())
}


class SnapshotEntityRequest extends Actor {
  def receive = {
    case commit: SnapshotEntityService.Commit =>
      commit("entity")
      context.stop(self)
  }
}

