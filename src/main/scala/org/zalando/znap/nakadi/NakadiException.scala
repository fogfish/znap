/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.nakadi

/**
  * Exception related to Nakadi.
  * @param msg message.
  * @param cause original exception.
  */
class NakadiException(msg: String, cause: Throwable) extends Exception(msg, cause) {
  def this(cause: Throwable) = this(null, cause)
}
