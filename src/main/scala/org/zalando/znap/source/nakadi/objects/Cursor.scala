/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.source.nakadi.objects

import com.fasterxml.jackson.annotation.JsonProperty

final case class Cursor(@JsonProperty(value = "partition", required = true)
                        partition: String,

                        @JsonProperty(value = "offset", required = true)
                        offset: String)
