/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.objects

final case class Partition(partition: String,
                           oldestAvailableOffset: String,
                           newestAvailableOffset: String)
