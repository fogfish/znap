package org.zalando.znap.disk

import java.io._

import akka.actor.SupervisorStrategy.Stop
import akka.actor.{ActorLogging, Props, OneForOneStrategy, Actor}
import org.zalando.znap.nakadi.NakadiReader.Ack
import scala.concurrent.duration._

/** Hierarchical Hash File System */
object HashFS {
  sealed trait KeyVal
  case class Put(id: String, blob: String) extends KeyVal
  case class Remove(id: String) extends  KeyVal
}

class HashFS(val root: File) extends Actor {
  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 0, withinTimeRange = 1 seconds) {
      case _: Exception => Stop
    }

  def receive = {
    case request: HashFS.KeyVal =>
      context.actorOf(Props(new HashTx(root))) forward request
  }
}

private
class HashTx(val root: File) extends Actor with ActorLogging {
  val hash = java.security.MessageDigest.getInstance("SHA-1")

  def receive = {
    case HashFS.Put(id: String, blob: String) =>
      log.debug(s"put $id")
      put(id, blob)
      sender() ! Ack
      context.stop(self)

    case HashFS.Remove(id: String) =>
      log.debug(s"remove $id")
      remove(id)
      sender() ! Ack
      context.stop(self)
  }

  private
  def put(id: String, blob: String) =
    try
    {
      val f = file(id)
      ensureDir(f)

      val out = new BufferedWriter(new FileWriter(f))
      out.write(blob)
      out.close()
    } catch {
      case e: IOException =>
        log.error(e, s"put $id to file is failed")
    }

  private
  def remove(id: String) =
    try
    {
      val f = file(id)
      f.delete()
    } catch {
      case e: IOException =>
        log.error(e, s"remove $id file is failed")
    }


  private
  def file(id: String): File = {
    val key = hash.digest(id.getBytes("UTF-8"))
      .map("%02x".format(_))
      .mkString

    val dir = new File(root, key.substring(0, 2))
    new File(dir, key)
  }

  private
  def ensureDir(file: File) = {
    if (!file.getParentFile().exists())
      file.getParentFile().mkdir()
  }

}

