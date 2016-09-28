/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.pipeline

import akka.actor.{Actor, ActorLogging, Props}
import akka.stream.actor._
import org.slf4j.LoggerFactory
import org.zalando.znap.config.SnapshotPipeline
import org.zalando.znap.persistence.OffsetWriterSync
import org.zalando.znap.source.nakadi.objects.{Cursor, EventBatch}
import org.zalando.znap.utils.NoUnexpectedMessages

import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

sealed trait OffsetWriterActor {
  protected val snapshotPipeline: SnapshotPipeline
  protected val offsetWriter: OffsetWriterSync

  val logger = LoggerFactory.getLogger(classOf[OffsetWriterActor])

  protected def write(cursor: Cursor): Unit = {
    try {
      offsetWriter.write(cursor)
    } catch {
      case NonFatal(e) =>
        logger.error(s"Error writing offset $cursor for pipeline ${snapshotPipeline.id}")
    }
  }
}

private class PeriodicOffsetWriterActor(override protected val snapshotPipeline: SnapshotPipeline,
                                        writePeriod: FiniteDuration,
                                        override protected val offsetWriter: OffsetWriterSync)
    extends Actor with ActorSubscriber with OffsetWriterActor with NoUnexpectedMessages with ActorLogging {
  import OffsetWriterActor._

  override protected def requestStrategy: RequestStrategy = WatermarkRequestStrategy(16)

  private var lastCursor: Option[Cursor] = None

  override def preStart(): Unit = {
    context.system.scheduler.schedule(
      writePeriod, writePeriod, self, Tick)(context.system.dispatcher)
  }

  override def receive: Receive = {
    case ActorSubscriberMessage.OnNext(cursor: Cursor) =>
      lastCursor = Some(cursor)

    case ActorSubscriberMessage.OnComplete =>
      log.info(s"PeriodicOffsetWriterActor for pipeline ${snapshotPipeline.id} got OnComplete, stopping.")
      context.stop(self)

    case ActorSubscriberMessage.OnError(e) =>
      log.info(s"PeriodicOffsetWriterActor for pipeline ${snapshotPipeline.id} got OnError, stopping.")
      context.stop(self)

    case Tick =>
      if (lastCursor.nonEmpty) {
        write(lastCursor.get)
      }
  }
}

private class ImmediateOffsetWriterActor(override protected val snapshotPipeline: SnapshotPipeline,
                                         override protected val offsetWriter: OffsetWriterSync)
    extends Actor with ActorSubscriber with OffsetWriterActor with NoUnexpectedMessages with ActorLogging {

  override protected def requestStrategy: RequestStrategy = WatermarkRequestStrategy(16)

  override def receive: Receive = {
    case ActorSubscriberMessage.OnNext(cursor: Cursor) =>
      write(cursor)
      request(1)

    case ActorSubscriberMessage.OnComplete =>
      log.info(s"ImmediateOffsetWriterActor for pipeline ${snapshotPipeline.id} got OnComplete, stopping.")
      context.stop(self)

    case ActorSubscriberMessage.OnError(e) =>
      log.info(s"ImmediateOffsetWriterActor for pipeline ${snapshotPipeline.id} got OnError, stopping.")
      context.stop(self)
  }
}

object OffsetWriterActor {
  case object Tick

  /**
    * @param writePeriod offset write period. If None, immediate write will be performed.
    */
  def props(snapshotPipeline: SnapshotPipeline,
            writePeriod: Option[FiniteDuration],
            offsetWriter: OffsetWriterSync): Props = {
    writePeriod match {
      case Some(period) =>
        Props(classOf[PeriodicOffsetWriterActor], snapshotPipeline, period, offsetWriter)
      case None =>
        Props(classOf[ImmediateOffsetWriterActor], snapshotPipeline, offsetWriter)
    }
  }
}