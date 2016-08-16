/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.signalling.kinesis

import java.nio.ByteBuffer

import com.amazonaws.services.kinesis.producer.KinesisProducer

class KinesisSignaller(kinesisProducer: KinesisProducer,
                       streamName: String) {
  import KinesisSignaller._

  import scala.collection.JavaConverters._

//  override def signal(value: String): Unit = ???

  def signal(keyValuePairs: List[(Key, Value)]): Unit = {
    // TODO normal groupping (see limits) and error processing.

    val futures = keyValuePairs.map { case (key, value) =>
      kinesisProducer.addUserRecord(streamName, key, ByteBuffer.wrap(value))
    }

    futures.foreach { f =>
      val result = f.get()
      if (!result.isSuccessful) {
        result.getAttempts.asScala.collectFirst {
          case a if !a.isSuccessful => a
        }.foreach { a => throw new Exception(s"Error writing to Kinesis: ${a.getErrorCode}: ${a.getErrorMessage}") }
      }
    }
  }
}

object KinesisSignaller {
  type Key = String
  type Value = Array[Byte]
}