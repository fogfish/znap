/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.pipeline

import java.time.ZonedDateTime

import akka.actor.{Actor, ActorLogging}
import akka.stream.{ActorMaterializer, KillSwitch}
import org.zalando.znap.PipelineId
import org.zalando.znap.config.Config
import org.zalando.znap.source.nakadi.NakadiTokens
import org.zalando.znap.utils.{NoUnexpectedMessages, ThrowableUtils, TimePeriodEventTracker}

/**
  * Actor that supervises pipelines.
  */
class PipelineManager(tokens: NakadiTokens) extends Actor with NoUnexpectedMessages with ActorLogging {
  import akka.pattern.pipe
  import context.dispatcher

  private implicit val mat = ActorMaterializer()

  private val pipelineBuilder = new PipelineBuilder(tokens)(context.system)

  private val pipelineConfigs = Config.Pipelines.map(t => t.id -> t).toMap
  private var pipelines = Map.empty[String, RunnablePipeline]
  private var runningPipelines = Set.empty[RunnablePipeline]
  private var killSwitches = Map.empty[String, KillSwitch]

  private val errorTracker = new TimePeriodEventTracker(
    Config.Supervision.Pipelines.MaxFailures,
    Config.Supervision.Pipelines.Period
  )

  override def preStart(): Unit = {
    Config.Pipelines.foreach { snapshotPipeline =>
      val pipelineId = snapshotPipeline.id

      pipelineBuilder.build(pipelineId, snapshotPipeline).foreach {
        case (partitionId, pipeline) =>
          pipelines += (pipelineId + partitionId) -> pipeline
          startPipeline(pipelineId, partitionId)
      }
    }
  }

  private def startPipeline(pipelineId: PipelineId, partitionId: String): Unit = {
    val pipeline = pipelines(pipelineId + partitionId)
    assertNotRunning(pipeline)
    runningPipelines += pipeline

    val (killSwitch, completionFuture) = pipeline.run()
    killSwitches += (pipelineId + partitionId) -> killSwitch
    completionFuture pipeTo self
    log.info(s"Pipeline $pipelineId for partition $partitionId started")
  }

  override def receive: Receive = {
    case p @ PipelineFinished(pipelineId, partitionId) =>
      log.error(s"Pipeline finishing is not expected, shutting down.")

      val pipeline = pipelines(pipelineId + partitionId)
      assertRunning(pipeline)
      runningPipelines -= pipeline

      killSwitches.foreach { case (_, killSwitch) =>
        killSwitch.shutdown()
      }
      throw new Exception("Pipeline finishing is not expected.")

    case p @ PipelineFailed(pipelineId, partitionId, cause) =>
      val tooManyErrors = errorTracker.registerEvent(ZonedDateTime.now())

      if (tooManyErrors) {
        log.error(s"Pipeline $pipelineId for partition $partitionId failed: ${ThrowableUtils.getStackTraceString(cause)}.")

        val errorMessage = s"Too many pipeline fails in last ${errorTracker.period}."
        log.error(errorMessage)

        val pipeline = pipelines(pipelineId + partitionId)
        assertRunning(pipeline)
        runningPipelines -= pipeline

        killSwitches.foreach { case (_, killSwitch) =>
          killSwitch.shutdown()
        }

        throw new Exception(errorMessage)
      } else {
        log.error(s"Pipeline $pipelineId for partition $partitionId failed, restarting: ${ThrowableUtils.getStackTraceString(cause)}.")

        val pipeline = pipelines(pipelineId + partitionId)
        assertRunning(pipeline)
        runningPipelines -= pipeline

        startPipeline(pipelineId, partitionId)
      }
  }

  private def assertRunning(pipeline: RunnablePipeline): Unit = {
    assert(runningPipelines.contains(pipeline))
  }
  private def assertNotRunning(pipeline: RunnablePipeline): Unit = {
    assert(!runningPipelines.contains(pipeline))
  }
}
