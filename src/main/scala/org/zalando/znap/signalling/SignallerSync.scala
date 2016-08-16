/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.signalling

trait SignallerSync {
  def signal(value: String): Unit
  def signal(values: List[String]): Unit
}
