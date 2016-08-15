/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.dump

import akka.Done
import akka.actor.{Actor, ActorLogging}
import akka.stream.scaladsl.{Flow, Keep, Sink, RunnableGraph}
import akka.stream.{ActorAttributes, ActorMaterializer}
import com.amazonaws.services.sqs.AmazonSQSClient
import org.zalando.znap.config.{Config, SnapshotTarget, SqsSignalling}
import org.zalando.znap.service.SnapshotService
import org.zalando.znap.signalling.sqs.SqsSignaller
import org.zalando.znap.source.nakadi.NakadiTokens
import org.zalando.znap.utils.{NoUnexpectedMessages, ThrowableUtils}

import scala.concurrent.Future

class SqsDumpRunner(tokens: NakadiTokens,
                    target: SnapshotTarget) extends Actor with NoUnexpectedMessages with ActorLogging {

  import SqsDumpRunner._
  import akka.pattern.pipe

  private implicit val ec = context.dispatcher

  override def preStart(): Unit = {
    implicit val mat = ActorMaterializer()
    dumpGraph.run() pipeTo self
  }

  override def receive: Receive = {
    case Done =>
      context.parent ! Finished
      context.stop(self)

    case akka.actor.Status.Failure(ex) =>
      log.error(s"Error in SQS dump stream for target ${target.id}: ${ThrowableUtils.getStackTraceString(ex)}")
      throw ex
  }

  private val dumpGraph: RunnableGraph[Future[Done]] = {
    val source = SnapshotService.getSnapshotKeys(target)

    val sqsClient = new AmazonSQSClient()
    val sqsSignalling = target.signalling.get.asInstanceOf[SqsSignalling]
    val signaller = new SqsSignaller(sqsSignalling, sqsClient)
    val signallingStage = Flow[String].map { key =>
      signaller.signal(key)
      key
    }
      .addAttributes(ActorAttributes.dispatcher(Config.Akka.SqsDispatcher))
      .async

    val sink = Sink.ignore

    source
      .via(signallingStage)
      .toMat(sink)(Keep.right)
  }
}

object SqsDumpRunner {
  case object Finished
}
