package org.zalando.znap.disk

import java.io.{BufferedWriter, File, FileWriter, IOException}

import akka.actor.{Actor, ActorLogging}
import org.zalando.znap.nakadi.Messages.Ack
import org.zalando.znap.utils.{EscalateEverythingSupervisorStrategy, NoUnexpectedMessages}

private class OffsetWriter(val root: File) extends Actor with NoUnexpectedMessages with ActorLogging {
  import OffsetWriter._

  override def receive: Receive = {
    case WriteOffset(partition, offset) =>
      val partitionFile = new File(root, s"partition_$partition")
      try
      {
        val out = new BufferedWriter(new FileWriter(partitionFile))
        out.write(offset)
        out.close()
      } catch {
        case e: IOException =>
          val message = s"Error writing offset $offset"
          log.error(e, message)
          throw new DiskException(message, e)
      }
      sender() ! Ack
  }
}

object OffsetWriter {
  final case class WriteOffset(partition: String, offset: String)
}
