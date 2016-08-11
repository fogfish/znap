/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.signalling

import scala.concurrent.Future

trait Signaller {
  def signal(values: List[String]): Future[Unit]
}
