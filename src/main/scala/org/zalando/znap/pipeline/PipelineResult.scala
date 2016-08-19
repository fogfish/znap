/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.pipeline

sealed trait PipelineResult {
  val id: String
  val pipelineInstanceId: String
}

final case class PipelineFinished(id: String,
                                  partition: String,
                                  pipelineInstanceId: String) extends PipelineResult
final case class PipelineFailed(id: String,
                                partition: String,
                                pipelineInstanceId: String,
                                cause: Throwable) extends PipelineResult
