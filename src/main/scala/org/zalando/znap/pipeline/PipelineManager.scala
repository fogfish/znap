/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.pipeline

import java.util.UUID

import akka.actor.{Actor, ActorLogging}
import akka.stream.{ActorMaterializer, KillSwitch}
import org.zalando.znap.config.{Config, SnapshotTarget}
import org.zalando.znap.source.nakadi.NakadiTokens
import org.zalando.znap.utils.{NoUnexpectedMessages, ThrowableUtils}

/**
  * Actor that supervises pipelines.
  */
class PipelineManager(tokens: NakadiTokens) extends Actor with NoUnexpectedMessages with ActorLogging {
  import akka.pattern.pipe
  import context.dispatcher

  private implicit val mat = ActorMaterializer()

  private val pipelineBuilder = new PipelineBuilder(tokens)(context.system)

  private val targets = Config.Targets.map(t => t.id -> t).toMap
  private var killSwitches = Map.empty[String, KillSwitch]
  override def preStart(): Unit = {
    Config.Targets.foreach(startPipeline)
  }

  private var pipelineInstances = Map.empty[String, String]

  private def startPipeline(snapshotTarget: SnapshotTarget): Unit = {
    val id = snapshotTarget.id
    val pipelineInstanceId = UUID.randomUUID().toString

    pipelineBuilder.build(id, pipelineInstanceId, snapshotTarget).foreach {
      case (partitionId, pipeline) =>
        log.info(s"Pipeline $id for partition $partitionId started")
        val (killSwitch, completionFuture) = pipeline.run()
        killSwitches += (id + partitionId) -> killSwitch
        pipelineInstances += (id + partitionId) -> pipelineInstanceId
        completionFuture pipeTo self
    }
  }

  override def receive: Receive = {
    case p @ PipelineFinished(id, partitionId, pipelineInstanceId) if sender() == self =>
      val registeredPipelineInstanceId = pipelineInstances(id + partitionId)
      assert(registeredPipelineInstanceId == pipelineInstanceId)

      log.error(s"Got $p, but pipelines should never finish, shutting down.")
      killSwitches.foreach { case (_, killSwitch) =>
        killSwitch.shutdown()
      }
      context.stop(self)

    case p @ PipelineFailed(id, partitionId, pipelineInstanceId, cause) if sender() == self =>
      val registeredPipelineInstanceId = pipelineInstances(id + partitionId)
      assert(registeredPipelineInstanceId == pipelineInstanceId)

      log.error(s"Pipeline $id for partition $partitionId failed with ${ThrowableUtils.getStackTraceString(cause)}, restarting.")
      val target = targets(id + partitionId)
      startPipeline(target)
  }
}
