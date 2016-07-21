/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.utils

import akka.stream.Materializer
import akka.stream.scaladsl.Source
import akka.util.ByteString

import scala.concurrent.{ExecutionContext, Future}

object RichStream {
  implicit class RichByteStringSource(source: Source[ByteString, Any]) {
    def collectAsString()(implicit mat: Materializer): Future[String] = {
      source.runFold("")((acc, bs) => acc + "" + bs.utf8String)
    }

    def collectAsObject[T : Manifest]()(implicit mat: Materializer, ec: ExecutionContext): Future[T] = {
      source.runFold("")((acc, bs) => acc + " " + bs.utf8String)
        .map(Json.read[T])
    }
  }
}
