/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.pipeline

sealed trait PipelineResult {
  val targetId: String
}

final case class PipelineFinished(targetId: String,
                                  partition: String) extends PipelineResult
final case class PipelineFailed(targetId: String,
                                partition: String,
                                cause: Throwable) extends PipelineResult
