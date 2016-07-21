/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.utils

import akka.util.Helpers

import scala.util.Random

object ActorNames {
  def randomPart(): String = {
    Helpers.base64(Random.nextLong())
  }
}
