/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.dumping.sqs

import com.amazonaws.services.sqs.AmazonSQSClient
import org.zalando.znap.config.SqsDumping
import org.zalando.znap.sqs.SqsBase

class SqsDumper(dumping: SqsDumping,
                sqsClient: AmazonSQSClient) extends SqsBase(dumping.uri.toString, sqsClient) {

  def dump(value: String): Unit = {
    send(List(value))
  }

  def dump(values: Seq[String]): Unit = {
    send(values)
  }
}
