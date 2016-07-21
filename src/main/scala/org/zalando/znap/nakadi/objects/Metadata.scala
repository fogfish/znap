/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.nakadi.objects

import java.time.ZonedDateTime

import com.fasterxml.jackson.annotation.JsonProperty

final case class Metadata(@JsonProperty(value = "occurred_at", required = true)
                          occuredAt: ZonedDateTime,

                          @JsonProperty(value = "eid", required = true)
                          eid: String,

                          @JsonProperty(value = "event_type", required = true)
                          eventType: String,

                          @JsonProperty(value = "flow_id", required = true)
                          flowId: String,

                          @JsonProperty(value = "received_at", required = true)
                          receivedAt: ZonedDateTime)
