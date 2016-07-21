/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.config

import java.io.File
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

import scala.concurrent.duration.FiniteDuration

class Config(snapshotsConfigFile: String) {
  private val logger = LoggerFactory.getLogger(classOf[Config])

  val ApplicationInstanceId = {
    val now = LocalDateTime.now(ZoneId.of("UTC"))
    val random = UUID.randomUUID().toString.replace("-", "").take(12)
    val format = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss")
    s"${format.format(now)}-$random"
  }

  // Application config.

  private val appConfig = ConfigFactory.systemProperties()
    .withFallback(ConfigFactory.defaultApplication())
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
    val SnapshotInitTimeout = {
      val t = appConfig.getDuration("persistence.snapshotInitTimeout")
      FiniteDuration(t.toMillis, TimeUnit.MILLISECONDS)
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

  private val snapshotsConfig = ConfigFactory.parseFile(new File(snapshotsConfigFile))

  object Paths {
    val WorkingDirectory = snapshotsConfig.getString("workingDirectory")
    val SnapshotsDirectory = snapshotsConfig.getString("snapshotsDirectory")
  }

  // TODO robust targets parsing
  val Targets: List[SnapshotTarget] = {
    import scala.collection.JavaConverters._
    for {
      targetObj <- snapshotsConfig.getObjectList("targets").asScala.toList
    } yield {
      val targetConf = targetObj.toConfig
      val targetType = targetConf.getString("type")

      targetType match {
        case "nakadi" =>
          val host = targetConf.getString("host")
          val secureConnection = targetConf.getBoolean("secureConnection")
          val port =
            if (targetConf.hasPath("port")) {
              targetConf.getInt("port")
            } else {
              if (secureConnection) {
                443
              } else {
                80
              }
            }

          val eventType = targetConf.getString("eventType")
          NakadiTarget(host, port, secureConnection, eventType)
        case tt =>
          val message = s"Unknown target type $tt"
          logger.error(message)
          throw new Exception(message)
      }
    }
  }
}
