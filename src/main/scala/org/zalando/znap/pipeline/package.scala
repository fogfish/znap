/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap

import akka.stream.UniqueKillSwitch
import akka.stream.scaladsl.RunnableGraph

import scala.concurrent.Future

package object pipeline {
  type Pipeline = RunnableGraph[(UniqueKillSwitch, Future[PipelineResult])]
}
