/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.source.nakadi.objects

import com.fasterxml.jackson.annotation.JsonProperty

final case class NakadiPartition(@JsonProperty(value = "partition", required = true)
                                 partition: String,

                                 @JsonProperty(value = "oldest_available_offset", required = true)
                                 oldestAvailableOffset: String,

                                 @JsonProperty(value = "newest_available_offset", required = true)
                                 newestAvailableOffset: String)
