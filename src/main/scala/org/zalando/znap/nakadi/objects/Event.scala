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
final case class Event(//@JsonProperty(value = "metadata", required = true)
                       //metadata: Metadata,

                       @JsonProperty(value = "data_op", required = true)
                       dataOp: String,

                       @JsonProperty(value = "data", required = true)
                       data: JsonNode,

                       @JsonProperty(value = "data_type", required = true)
                       dataType: String)
