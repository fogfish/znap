/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.nakadi

import java.time.ZonedDateTime

import akka.actor.SupervisorStrategy.{Escalate, Stop}
import akka.actor.{ActorLogging, ActorRef, FSM, OneForOneStrategy, Props, SupervisorStrategy}
import akka.http.scaladsl.model.EntityStreamSizeException
import NakadiReader._
import org.zalando.znap.config.{Config, NakadiSource}
import org.zalando.znap.nakadi.Messages.Ack
import org.zalando.znap.nakadi.objects.EventBatch
import org.zalando.znap.utils.{ActorNames, ThrowableUtils, TimePeriodEventTracker, UnexpectedMessageException}
import scala.concurrent.Future

import scala.util.control.NonFatal

/**
  * Actor that gives a reliable channel for consuming from Nakadi.
  *
  * It attempts to restart the consumption if it fails.
  */
class NakadiReader(partition: String,
                   offsetOpt: Option[String],
                   nakadiSource: NakadiSource,
                   tokens: NakadiTokens,
                   persistor: ActorRef) extends FSM[State, Unit] with ActorLogging {
  val errorTracker = new TimePeriodEventTracker(
    Config.Supervision.NakadiReader.MaxFailures,
    Config.Supervision.NakadiReader.Period
  )
  override def supervisorStrategy: SupervisorStrategy = {
    OneForOneStrategy() {
      // Stop consuming of the partition when
      // internal counter in HTTP connection reaches the limit.
      // (Now there is no way in Akka HTTP to disable this limit
      // for streaming responses.)
      // The actor will be started anew.
      case ex: EntityStreamSizeException =>
        log.info(s"NakadiReaderWorker consumed too much from the stream, restarting.")

        if (stateName == WaitingForSeq) {
          startWorker()
        } else {
          worker = None
        }

        Stop

      // Just restart in case of StreamCompleted.
      case _: StreamCompletedException =>
        startWorker()
        Stop

      // A decision about invalid offsets will be made upper.
      case _: InvalidOffsetException =>
        Escalate

      case NonFatal(ex) =>
        val tooManyErrors = errorTracker.registerEvent(ZonedDateTime.now())
        if (tooManyErrors) {
          log.error(s"Exception from NakadiReaderWorker. " +
            s"Too many exceptions in last ${errorTracker.period}: ${ThrowableUtils.getStackTraceString(ex)}")
          Escalate
        } else {
          log.warning(s"Exception from NakadiReaderWorker, " +
            s"will be restarted: ${ThrowableUtils.getStackTraceString(ex)}")

          if (stateName == WaitingForSeq) {
            startWorker()
          } else {
            worker = None
          }

          Stop
        }

      case _ =>
        Escalate
    }
  }

  var worker: Option[ActorRef] = None

  /**
    * The last offset (the last from a sequence of batches)
    * that was sent to the parent.
    */
  var currentSentOffset: Option[String] = None

  /**
    * The last offset that was acknowledged by the parent.
    */
  var lastAckedOffset: Option[String] = offsetOpt

    override def preStart(): Unit = {
      log.info(s"Nakadi reader for source ${nakadiSource.id} and partition $partition started")
      startWorker()
    }

    def startWorker(): Unit = {
      val workerRef = context.actorOf(
        Props(classOf[NakadiReaderWorker], partition, lastAckedOffset, nakadiSource, tokens),
        s"NakadiReaderWorker-${nakadiSource.id}-$partition-${ActorNames.randomPart()}"
      )
      worker = Some(workerRef)
    }

  startWith(WaitingForSeq, ())

  when(WaitingForSeq) {
    // An echo from a previous worker.
    case Event(_: EventBatch, _) if !worker.contains(sender()) =>
      stay()

    case Event(batch: EventBatch, _) =>
      implicit val ec = context.dispatcher
      currentSentOffset = Some(batch.cursor.offset)
      persistor ! batch

      goto(WaitingForAck)
  }

  when(WaitingForAck) {
    // An echo from a previous worker.
    case Event(_: EventBatch, _) if !worker.contains(sender()) =>
      stay()

    case Event(a @ Ack, _) =>
      lastAckedOffset = currentSentOffset
      currentSentOffset = None

      if (worker.isEmpty) {
        startWorker()
      } else {
        worker.get ! a
      }

      goto(WaitingForSeq)
  }

  whenUnhandled {
    case Event(unexpected, _) =>
      log.error(s"Unexpected message $unexpected in state ${this.stateName} with data ${this.stateData} from ${sender()}")
      throw new UnexpectedMessageException(unexpected, sender())
  }
}

object NakadiReader {
  sealed trait State
  case object WaitingForSeq extends State
  case object WaitingForAck extends State

  final class StreamCompletedException extends Exception
}
