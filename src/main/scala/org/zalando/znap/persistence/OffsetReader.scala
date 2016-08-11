/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.persistence

import scala.concurrent.Future

trait OffsetReader {
  def getLastOffsets: Future[Map[String, String]]
}
