/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.config

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.apache.http.client.utils.URIBuilder
import org.slf4j.LoggerFactory

import scala.concurrent.duration.FiniteDuration

class Config {
  private val logger = LoggerFactory.getLogger(classOf[Config])

  val ApplicationInstanceId = {
    val now = LocalDateTime.now(ZoneId.of("UTC"))
    val random = UUID.randomUUID().toString.replace("-", "").take(12)
    val format = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss")
    s"${format.format(now)}-$random"
  }

  // Application config.

  object Akka {
    val DynamoDBDispatcher = "dynamodb-dispatcher"
  }

  private val appConfig = ConfigFactory
    .systemProperties()
    .withFallback(ConfigFactory.defaultApplication().resolve())

  object Tokens {
    val AccessToken = appConfig.getString("tokens.accessToken")
    val TokenInfo = appConfig.getString("tokens.tokenInfo")
  }

  object Nakadi {
    val PartitionsReadTimeout = {
      val t = appConfig.getDuration("nakadi.partitionsReadTimeout")
      FiniteDuration(t.toMillis, TimeUnit.MILLISECONDS)
    }
  }

  val DefaultAskTimeoutDuration = appConfig.getDuration("akka.defaultAskTimeout")
  val DefaultAskTimeout = Timeout(DefaultAskTimeoutDuration.toMillis, TimeUnit.MILLISECONDS)
  val HttpStreamingMaxSize = appConfig.getBytes("http.streamingMaxSize")

  object Persistence {
    object Disk {
      val SnapshotInitTimeout = {
        val t = appConfig.getDuration("persistence.disk.snapshotInitTimeout")
        FiniteDuration(t.toMillis, TimeUnit.MILLISECONDS)
      }

      val SnapshotInterval = {
        val t = appConfig.getDuration("persistence.disk.snapshotInterval")
        FiniteDuration(t.toMillis, TimeUnit.MILLISECONDS)
      }

      // TODO dirs part of persistence global config. (folders has to be backed by EBS)
      object Paths {
        val WorkingDirectory = appConfig.getString("persistence.disk.workingDirectory")
        val SnapshotsDirectory = appConfig.getString("persistence.disk.snapshotsDirectory")
      }
    }
  }

  object Supervision {

//    object RootExtractor {
//      val MaxFailures = config.getInt("extraction.supervision.rootExtractor.maxFailures")
//      val Period = config.getDuration("extraction.supervision.rootExtractor.period")
//    }

    object NakadiReader {
      val MaxFailures = appConfig.getInt("supervision.nakadiReader.maxFailures")
      val Period = appConfig.getDuration("supervision.nakadiReader.period")
    }

  }



  // Snapshots config.


  val Targets: List[SnapshotTarget] = {
    val snapshotsConfig = appConfig.getString("znap.streams").split('|').toList
    snapshotsConfig.map(x => parseSnapshotConfig(x))
  }
//  {
//    for {
//      uri <- snapshotsConfig
//    } yield {
//      val stream = ConfigStream(uri)
//
//      stream.protocol match {
//        case "nakadi" =>
//          NakadiTarget(stream.schema, stream.host, stream.port, stream.stream)
//
//        case tt =>
//          val message = s"Unknown target type $tt"
//          logger.error(message)
//          throw new Exception(message)
//      }
//    }
//  }

  private def parseSnapshotConfig(configString: String): SnapshotTarget = {
    import scala.collection.JavaConversions._

    val Array(protocol, rest) = configString.split("\\+", 2)
    protocol match {
      case "nakadi" =>
        val restURIBuilder = new URIBuilder(rest)
        val queryParams = restURIBuilder.getQueryParams.map { qp =>
          qp.getName -> qp.getValue
        }.toMap
        restURIBuilder.removeQuery()

        val eventType = restURIBuilder.getPath.drop(1)

        restURIBuilder.setPath("")
        restURIBuilder.setPort(resolvePort(restURIBuilder.getScheme, restURIBuilder.getPort))
        val nakadiURI = restURIBuilder.build()

        val eventClass = queryParams("event-class")
        val key = queryParams("key").split('.').toList
        val source = NakadiSource(nakadiURI, eventType, eventClass)

        val dest = parseDestinaton(queryParams("to"))

        SnapshotTarget(source, dest, key)
    }
  }

  private def parseDestinaton(destString: String): SnapshotDestination = {
    val Array(protocol, rest) = destString.split("\\+", 2)
    protocol match {
      case "dynamodb" =>
        val restURIBuilder = new URIBuilder(rest)
        restURIBuilder.setPort(resolvePort(restURIBuilder.getScheme, restURIBuilder.getPort))
        DynamoDBDestination(restURIBuilder.build())
    }
  }

  private def resolvePort(scheme: String, port: Int): Int =
    (scheme, port) match {
      case ("http", -1)  => 80
      case ("https", -1) => 443
      case _ => port
    }

  object DynamoDB {
    object OffsetsTable {
      val Name = "offsets"

      object Attributes {
        val TargetId = "target_id"
        val Partition = "partition"
        val Offset = "offset"
      }
    }

    object KVTables {
      object Attributes {
        val Key = "key"
        val Value = "value"
      }
    }

    object Batches {
      val WriteBatchSize = 25
    }
  }
}
