package org.zalando.znap.service.stream

import akka.actor.Props
import org.zalando.scarl.Supervisor
import org.zalando.scarl.Supervisor.Specs
import org.zalando.znap.config.{NakadiSource, Config, SnapshotTarget}
import org.zalando.znap.nakadi.{NakadiTargetSnapshotterRoot, OAuth}
import scala.concurrent.duration._

/** Ingress Data Stream(s) SubSystem
  */
object StreamService {
  private val uid = classOf[StreamService].getSimpleName

  /** specification of snapshot subsystem
    */
  def spec(oauth: OAuth): Specs = {
    Specs(uid, Props(classOf[StreamService], Config.Targets, oauth))
  }
}


class StreamService(list: List[SnapshotTarget], oauth: OAuth) extends Supervisor {

  override
  def supervisorStrategy = strategyOneForOne(4, 1800.seconds)

  def specs = list.map {
    case target@SnapshotTarget(id, source: NakadiSource, _, _, _) =>
      Specs(id, Props(classOf[NakadiTargetSnapshotterRoot], target, oauth))
  }
}

