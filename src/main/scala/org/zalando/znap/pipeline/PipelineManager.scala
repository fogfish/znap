/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.pipeline

import akka.actor.{Actor, ActorLogging}
import akka.stream.{ActorMaterializer, KillSwitch}
import org.zalando.znap.TargetId
import org.zalando.znap.config.Config
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
  private var pipelines = Map.empty[String, Pipeline]
  private var runningPipelines = Set.empty[Pipeline]
  private var killSwitches = Map.empty[String, KillSwitch]

  override def preStart(): Unit = {
    Config.Targets.foreach { snapshotTarget =>
      val targetId = snapshotTarget.id

      pipelineBuilder.build(targetId, snapshotTarget).foreach {
        case (partitionId, pipeline) =>
          pipelines += (targetId + partitionId) -> pipeline
          startPipeline(targetId, partitionId)
      }
    }
  }

  private def startPipeline(targetId: TargetId, partitionId: String): Unit = {
    val pipeline = pipelines(targetId + partitionId)
    assertNotRunning(pipeline)
    runningPipelines += pipeline

    val (killSwitch, completionFuture) = pipeline.run()
    killSwitches += (targetId + partitionId) -> killSwitch
    completionFuture pipeTo self
    log.info(s"Pipeline $targetId for partition $partitionId started")
  }

  override def receive: Receive = {
    case p @ PipelineFinished(targetId, partitionId) =>
      val pipeline = pipelines(targetId + partitionId)
      assertRunning(pipeline)
      runningPipelines -= pipeline

      killSwitches.foreach { case (_, killSwitch) =>
        killSwitch.shutdown()
      }
      context.stop(self)

    case p @ PipelineFailed(targetId, partitionId, cause) =>
      log.error(s"Pipeline $targetId for partition $partitionId failed with ${ThrowableUtils.getStackTraceString(cause)}, restarting.")

      val pipeline = pipelines(targetId + partitionId)
      assertRunning(pipeline)
      runningPipelines -= pipeline

      startPipeline(targetId, partitionId)
  }

  private def assertRunning(pipeline: Pipeline): Unit = {
    assert(runningPipelines.contains(pipeline))
  }
  private def assertNotRunning(pipeline: Pipeline): Unit = {
    assert(!runningPipelines.contains(pipeline))
  }
}
