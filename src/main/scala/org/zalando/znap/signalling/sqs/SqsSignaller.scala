/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.signalling.sqs

import com.amazonaws.services.sqs.AmazonSQSClient
import org.zalando.znap.config.SqsSignalling
import org.zalando.znap.sqs.SqsBase

class SqsSignaller(signalling: SqsSignalling,
                   sqsClient: AmazonSQSClient) extends SqsBase(signalling.uri.toString, sqsClient) {
  def signal(values: List[String]): Unit = {
    send(values)
  }
}

object SqsSignaller {
}