/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.nakadi.objects

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}
import com.fasterxml.jackson.databind.JsonNode

@JsonIgnoreProperties(ignoreUnknown=true)
final case class NakadiEvent(@JsonProperty(value = "event_class", required = true)
                             eventClass: String,

                             @JsonProperty(value = "body", required = true)
                             body: JsonNode,

                             @JsonProperty(value = "event_type", required = true)
                             eventType: String)
