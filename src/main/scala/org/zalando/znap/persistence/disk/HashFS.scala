/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.persistence.disk

import java.io._

import akka.actor.{Actor, ActorLogging, ActorRef, FSM, Props}
import org.zalando.znap.persistence.disk.HashFS._
import org.zalando.znap.nakadi.Messages.Ack
import org.zalando.znap.utils.{EscalateEverythingSupervisorStrategy, NoUnexpectedMessages, UnexpectedMessageException}

/**
  * Hierarchical Hash File System.
  */
class HashFS(root: File) extends FSM[State, Data] with ActorLogging {
  override val supervisorStrategy = new EscalateEverythingSupervisorStrategy

  startWith(WaitingForCommands, NoData)

  when(WaitingForCommands) {
    case Event(Commands(Nil), NoData) =>
      log.debug("Got empty command list, just ack")
      sender() ! Ack
      stay()

    case Event(Commands(commands), NoData) =>
      log.debug("Got non-empty list of commands")

      commands.foreach { command =>
        context.actorOf(Props(classOf[HashFSTx], root)) ! command
      }

      goto(ExecutingCommands) using CommandsToExecute(commands.toSet, sender())
  }

  when(ExecutingCommands) {
    case Event(CommandExecuted(commandExecuted), CommandsToExecute(commands, commandsSender)) =>
      val newCommands = commands - commandExecuted
      if (newCommands.isEmpty) {
        commandsSender ! Ack
        goto (WaitingForCommands) using NoData
      } else {
        stay() using CommandsToExecute(newCommands, commandsSender)
      }
  }


  whenUnhandled {
    case Event(unexpected, _) =>
      log.error(s"Unexpected message $unexpected in state ${this.stateName} with data ${this.stateData} from ${sender()}")
      throw new UnexpectedMessageException(unexpected, sender())
  }
}

object HashFS {
  sealed trait State
  private case object WaitingForCommands extends State
  private case object ExecutingCommands extends State

  sealed trait Data
  private case object NoData extends Data
  private final case class CommandsToExecute(commands: Set[Command], commandsSender: ActorRef) extends Data

  sealed trait Command
  final case class PutCommand(id: String, blob: String) extends Command
  final case class RemoveCommand(id: String) extends Command

  final case class Commands(commands: List[Command])

  final case class CommandExecuted(command: Command)
}

private class HashFSTx(val root: File) extends Actor with NoUnexpectedMessages with ActorLogging {
  import HashFSTx.hash

  override def receive: Receive = {
    case command @ PutCommand(id, blob) =>
      put(id, blob)
      context.parent ! CommandExecuted(command)
      context.stop(self)

    case command @ RemoveCommand(id) =>
      remove(id)
      context.parent ! CommandExecuted(command)
      context.stop(self)
  }

  private def put(id: String, blob: String): Unit = {
//      log.debug(s"Put command for id = $id")

    try
    {
      val f = file(id)
      ensureDir(f)

      val out = new BufferedWriter(new FileWriter(f))
      out.write(blob)
      out.close()
    } catch {
      case e: IOException =>
        val message = s"Put $id to file failed"
        log.error(e, message)
        throw new DiskException(message, e)
    }
  }

  private def remove(id: String): Unit = {
//      log.debug(s"Remove command for id = $id")

    try
    {
      val f = file(id)
      f.delete()
    } catch {
      case e: IOException =>
        val message = s"Remove $id file failed"
        log.error(e, message)
        throw new DiskException(message, e)
    }
  }

  private def file(id: String): File = {
    val key = hash.digest(id.getBytes("UTF-8"))
      .map("%02x".format(_))
      .mkString

    val dir = new File(root, key.substring(0, 2))
    new File(dir, id)
  }

  private def ensureDir(file: File) = {
    if (!file.getParentFile.exists())
      file.getParentFile.mkdir()
  }
}

object HashFSTx {
  val hash = java.security.MessageDigest.getInstance("SHA-1")
}
