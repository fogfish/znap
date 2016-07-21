/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.nakadi.objects

import com.fasterxml.jackson.annotation.JsonProperty

final case class EventBatch(@JsonProperty(value = "cursor", required = true)
                            cursor: Cursor,

                            @JsonProperty(value = "events")
                            events: Option[List[Event]])
