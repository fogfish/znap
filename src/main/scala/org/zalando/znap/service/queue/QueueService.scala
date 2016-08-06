package org.zalando.znap.service.queue

import akka.actor._
import org.zalando.scarl.Supervisor.Specs
import org.zalando.znap.config.{SnapshotTarget, NakadiSource, Config}
import org.zalando.znap.nakadi.{NakadiQueueService, OAuth}
import scala.concurrent.Future
import scala.concurrent.duration._
import org.zalando.scarl.Supervisor
import java.net.URI


/** Ingress Queue SubSystem interface
  */
object QueueService {
  private val uid  = classOf[QueueService].getSimpleName
  private val path = "/user/znap/" + uid + "/"

  /** specification of queue subsystem
    */
  def spec(oauth: OAuth): Specs = {
    Specs(uid, Props(classOf[QueueService], Config.Targets, oauth))
  }

  /** lookup queue i/o pool
    */
  def pool(uri: URI)(implicit sys: ActorSystem): Future[ActorRef] = {
    sys.actorSelection(path + uri.getAuthority).resolveOne(5.seconds)
  }
}


class QueueService(list: List[SnapshotTarget], oauth: OAuth) extends Supervisor {
  override
  def supervisorStrategy = strategyOneForOne(2, 1200.seconds)

  def specs = list
    .groupBy {_.source.asInstanceOf[NakadiSource].uri.getAuthority}
    .map {_._2.head}
    .map {
      (x) =>
        val src = x.source.asInstanceOf[NakadiSource]
        Specs(src.uri.getAuthority, Props(classOf[NakadiQueueService], src, oauth))
    }
    .toSeq
}
