/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.utils

import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

object Json {
  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.registerModule(new JavaTimeModule())
  mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

  def read[T : Manifest](content: String): T = {
    mapper.readValue[T](content)
  }

  def write(ob: Any): String = {
    mapper.writeValueAsString(ob)
  }

  import scala.language.implicitConversions
  object Implicit {
    implicit def jsonifiable[T](ob: T): Jsonifiable[T] = new Jsonifiable[T](ob)

    final class Jsonifiable[T] private[Implicit](ob: T) {
      def toJson: String = Json.write(ob)
    }

  }

}
