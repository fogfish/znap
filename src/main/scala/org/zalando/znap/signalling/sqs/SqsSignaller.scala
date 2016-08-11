/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.signalling.sqs

import java.util.UUID

import com.amazonaws.services.sqs.AmazonSQSClient
import com.amazonaws.services.sqs.model.{SendMessageBatchRequest, SendMessageBatchRequestEntry}
import org.slf4j.LoggerFactory
import org.zalando.znap.config.{Config, SnapshotTarget, SqsSignalling}
import org.zalando.znap.signalling.SignallerSync

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}

class SqsSignaller(signalling: SqsSignalling,
                   sqsClient: AmazonSQSClient) extends SignallerSync {
  import collection.JavaConverters._
  import SqsSignaller._

  private val logger = LoggerFactory.getLogger(classOf[SqsSignaller])

  override def signal(values: List[String]): Unit = {
    if (values.nonEmpty) {
      values.foreach(v => assert(v.length <= Config.SQS.MaxMessageBodySize))

      signal0(values)(MaxRetries)
    }
  }

  @tailrec
  private def signal0(values: List[String])(retriesLeft: Int): Unit = {
    val (toWrite, rest) = values.span(TotalSizeAndCountPredicate())

    assert(toWrite.nonEmpty)

    val entriesMap = toWrite.map { value =>
      val id = UUID.randomUUID().toString.replaceAll("\\-", "")
      id -> new SendMessageBatchRequestEntry(id, value)
    }.toMap

    val entriesJava = entriesMap.values.toList.asJava
    val sendMessageBatchResult =
      sqsClient.sendMessageBatch(signalling.uri.toString, entriesJava)

    val failed = sendMessageBatchResult.getFailed.asScala.toList
    if (failed.nonEmpty) {
      if (retriesLeft > 0) {
        val toRetry = failed.map { e =>
          logger.info(s"Sqs sending error: ${e.getMessage}")
          entriesMap(e.getId).getMessageBody
        }
        signal0(toRetry ++ rest)(retriesLeft - 1)
      } else {
        val message = "Retries limit exceeded"
        logger.error(message)
        throw new Exception(message)
      }
    } else if (rest.nonEmpty) {
      // Don't decrease retriesLeft - not a retry.
      signal0(rest)(retriesLeft)
    }
  }
}

object SqsSignaller {
  private val MaxRetries = 5

  private val MaxTotalSize = 256 * 1024

  private case class TotalSizeAndCountPredicate() extends Function[String, Boolean] {
    private var totalSize = 0
    private var totalCount = 0
    private var shouldContinue = true
    override def apply(v1: String): Boolean = {
      if (!shouldContinue) {
        false
      } else {
        totalSize += v1.getBytes("UTF-8").length
        totalCount += 1
        if (totalSize <= MaxTotalSize && totalCount <= Config.SQS.MaxEntriesInWriteBatch) {
          true
        } else {
          shouldContinue = false
          false
        }
      }
    }
  }
}