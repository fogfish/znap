/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.source.nakadi

class InvalidOffsetException(val partition: String,
                             val offset: String,
                             val message: String) extends Exception {

}
