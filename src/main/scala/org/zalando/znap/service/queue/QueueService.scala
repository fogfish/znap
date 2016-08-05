package org.zalando.znap.service.queue

import akka.actor._
import org.zalando.znap.config.{SnapshotTarget, NakadiSource, Config}
import org.zalando.znap.nakadi.{NakadiQueueService, OAuth}
import scala.concurrent.duration._
import org.zalando.scarl.Supervisor

object QueueService {
  val id = "queues"

  def spec(target: List[SnapshotTarget], oauth: OAuth) =
    Props(classOf[QueueService], target, oauth)
    // todo: use the following definition with Root Supervisor
    //Supervisor.Supervisor(id, Props(classOf[QueueService]))

  def pool(id: String)(implicit sys: ActorSystem) =
    sys.actorSelection("/user/queues/" + id)
}

class QueueService(target: List[SnapshotTarget], oauth: OAuth) extends Supervisor {
  override
  def supervisorStrategy = strategyOneForOne(2, 1200.seconds)

  def init = target
    .groupBy {(x) => x.source.asInstanceOf[NakadiSource].uri.getAuthority}
    .map {_._2.head}
    .map {
      (x) =>
        x.source match {
          case src: NakadiSource =>
            Supervisor.Worker(src.uri.getAuthority, Props(classOf[NakadiQueueService], src, oauth))
        }
    }
    .toSeq
}
