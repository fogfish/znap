/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
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

