/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.utils

import java.io.{PrintWriter, StringWriter}

object ThrowableUtils {
  /**
    * Get stack trace of [[Throwable]] in the form of [[String]].
    */
  def getStackTraceString(throwable: Throwable): String = {
    val sw = new StringWriter()
    throwable.printStackTrace(new PrintWriter(sw))
    sw.flush()
    sw.toString
  }
}
