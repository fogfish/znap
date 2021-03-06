/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.config

import java.io.File
import java.net.URI
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.util.Timeout
import com.amazonaws.services.s3.AmazonS3Client
import com.typesafe.config.{ConfigException, ConfigParseOptions, Config => TypesafeConfig, ConfigFactory => TypesafeConfigFactory}
import org.apache.http.client.utils.URIBuilder
import org.slf4j.LoggerFactory

import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

object Config {
  import scala.collection.JavaConversions._

  /** root application config */
  private val appConfig = TypesafeConfigFactory
    .systemProperties()
    .withFallback(readInstanceConfig())
    .withFallback(TypesafeConfigFactory.defaultApplication().resolve())


  val ApplicationInstanceId = {
    val now = LocalDateTime.now(ZoneId.of("UTC"))
    val random = UUID.randomUUID().toString.replace("-", "").take(12)
    val format = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss")
    s"${format.format(now)}-$random"
  }

  // Application config.

  object Akka {
    val DynamoDBDispatcher = "dynamodb-dispatcher"
    val SqsDispatcher = "sqs-dispatcher"
    val KinesisDispatcher = "kinesis-dispatcher"
  }


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
    object DynamoDB {
      val OffsetWritePeriod = readOffsetWritePeriod(appConfig, "persistence.dynamodb.offset-write-period")
    }
  }

  private def readOffsetWritePeriod(appConfig: TypesafeConfig, path: String): Option[FiniteDuration] = {
    try {
      val t = appConfig.getDuration(path)
      Some(FiniteDuration(t.toMillis, TimeUnit.MILLISECONDS))
    } catch {
      case _: ConfigException.BadValue =>
        val v = appConfig.getString(path)
        if (v == "sync") {
          None
        } else {
          throw new Exception(s"Offset write period at $path can be a duration or 'sync'.")
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

    object Pipelines {
      val MaxFailures = appConfig.getInt("supervision.pipelines.maxFailures")
      val Period = appConfig.getDuration("supervision.pipelines.period")
    }
  }


  // Snapshots config.


  val Pipelines: List[SnapshotPipeline] = {
    val pipelines = appConfig.getObjectList("snapshotting.pipelines")
      .toList.map(co => readSnapshotPipeline(co.toConfig))

    // Check ids uniqueness.
    val allPipelineIds = pipelines.map(_.id)
    allPipelineIds.groupBy(x => x).foreach { case (id, lst) =>
      if (lst.size > 1) {
        throw new Exception(s"Pipeline id $id is not unique")
      }
    }
    val allTargetIds = pipelines.flatMap(_.targets.map(_.id))
    allTargetIds.groupBy(x => x).foreach { case (id, lst) =>
      if (lst.size > 1) {
        throw new Exception(s"Target id $id is not unique")
      }
    }

    pipelines
  }

  private def readSnapshotPipeline(configObject: TypesafeConfig): SnapshotPipeline = {
    val id = configObject.getString("id")

    val source = {
      val sourceConfig = configObject.getObject("source").toConfig
      sourceConfig.getString("type") match {
        case "nakadi" =>
          val nakadiURI = {
            val nakadiURIBuilder = new URIBuilder(sourceConfig.getString("url"))
            nakadiURIBuilder.setPort(resolvePort(nakadiURIBuilder.getScheme, nakadiURIBuilder.getPort))
            nakadiURIBuilder.build()
          }

          val batchLimit = Try(sourceConfig.getLong("batch-limit")).toOption
          val compress = Try(sourceConfig.getBoolean("compress")).toOption.getOrElse(true)

          val eventType = sourceConfig.getString("event-type")
          NakadiSource(nakadiURI, eventType, batchLimit, compress)
      }
    }

    val targets = configObject.getObjectList("targets").toList.map(o => readSnapshotTarget(o.toConfig))

    val offsetPersistence = {
      val offsetPersistenceConfig = configObject.getObject("offset-persistence").toConfig
      offsetPersistenceConfig.getString("type") match {
        case "dynamodb" =>
          val uriBuilder = new URIBuilder(offsetPersistenceConfig.getString("url"))
          uriBuilder.setPort(resolvePort(uriBuilder.getScheme, uriBuilder.getPort))

          val tableName = offsetPersistenceConfig.getString("table-name")
          DynamoDBOffsetPersistence(uriBuilder.build(), tableName)
      }
    }

    SnapshotPipeline(id, source, targets, offsetPersistence)
  }

  private def readSnapshotTarget(targetConfig: TypesafeConfig): SnapshotTarget = {
    val id = targetConfig.getString("id")

    val filter = Try(targetConfig.getObject("filter")) match {
      case Success(filterObject) =>
        val filterConfig = filterObject.toConfig
        if (filterConfig.entrySet().size() != 1) {
          throw new Exception("Filter can contain only one field")
        }

        val entry = filterConfig.entrySet().head
        val field = entry.getKey
        val values = filterConfig.getStringList(field).toSet
        Some(SourceFilter(field, values))

      case Failure(_: ConfigException.Missing) =>
        None

      case Failure(ex) =>
        throw ex
    }

    val key = targetConfig.getString("key").split('.').toList

    val destination = {
      val destConfig = targetConfig.getObject("destination").toConfig
      destConfig.getString("type") match {
        case "dynamodb" =>
          val uriBuilder = new URIBuilder(destConfig.getString("url"))
          uriBuilder.setPort(resolvePort(uriBuilder.getScheme, uriBuilder.getPort))

          val tableName = destConfig.getString("table-name")
          val compress = destConfig.getBoolean("compress")
          DynamoDBDestination(uriBuilder.build(), tableName, compress)
      }
    }

    val signalling = {
      Try(targetConfig.getObject("signalling")) match {
        case Success(signallingObject) =>
          val signallingConfig = signallingObject.toConfig
          signallingConfig.getString("type") match {
            case "sqs" =>
              val uri = new URI(signallingConfig.getString("url"))
              val publishType = parsePublishTypeString(signallingConfig.getString("publish-type"))
              Some(SqsSignalling(uri, publishType))

            case "kinesis" =>
              val amazonRegion = signallingConfig.getString("amazon-region")
              val streamName = signallingConfig.getString("stream")
              val publishType = parsePublishTypeString(signallingConfig.getString("publish-type"))
              Some(KinesisSignalling(amazonRegion, streamName, publishType))
          }

        case Failure(_: ConfigException.Missing) =>
          None

        case Failure(ex) =>
          throw ex
      }
    }

    val dumping = {
      val dumpingConfig = targetConfig.getObject("dumping").toConfig
      Try(dumpingConfig.getString("type")) match {
        case Success("sqs") =>
          val uri = new URI(dumpingConfig.getString("url"))
          val publishType = parsePublishTypeString(dumpingConfig.getString("publish-type"))
          Some(SqsDumping(uri, publishType))

        case Success("kinesis") =>
          val amazonRegion = dumpingConfig.getString("amazon-region")
          val streamName = dumpingConfig.getString("stream")
          val publishType = parsePublishTypeString(dumpingConfig.getString("publish-type"))
          Some(KinesisDumping(amazonRegion, streamName, publishType))

        case Failure(_: ConfigException.Missing) =>
          None

        case Failure(ex) =>
          throw ex
      }
    }

    SnapshotTarget(id, filter, key, destination, signalling, dumping)
  }

  private def parsePublishTypeString(publishTypeString: String): PublishType = {
    publishTypeString match {
      case "keys-only" => PublishType.KeysOnly
      case "events-uncompressed" => PublishType.EventsUncompressed
      case "events-compressed" => PublishType.EventsCompressed
      case other => throw new Exception(s"Unknown publish type $other")
    }
  }

  private def resolvePort(scheme: String, port: Int): Int =
    (scheme, port) match {
      case ("http", -1)  => 80
      case ("https", -1) => 443
      case _ => port
    }

  object DynamoDB {
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

  object SQS {
    val MaxMessageBodySize = 256 * 1024
    val MaxEntriesInWriteBatch = 10
  }


  private def readInstanceConfig(): TypesafeConfig = {
    val configFile = System.getenv("ZNAP_CONFIG_FILE")
    if (configFile == null) {
      throw new Exception("ZNAP_CONFIG_FILE environment variable must be set")
    } else {
      val parts = configFile.split("://", 2)
      if (parts.length < 2) {
//        logger.info(s"No scheme is set for config file URL, considered as file path.")
        readInstanceConfigFromFile(configFile)
      } else {
        parts(0).toLowerCase() match {
          case "s3" =>
            readInstanceConfigFromS3(configFile)

          case unknown =>
            throw new Exception(s"""Unknown scheme "$unknown" for config file URL: "$configFile"""")
        }
      }
    }
  }

  private def readInstanceConfigFromS3(configFile: String): TypesafeConfig = {
    val Array(_, path) = configFile.split("://", 2)
    val parts = path.split("/", 2)
    if (parts.length < 2) {
      throw new Exception(s"""Incorrect format of s3 URL for config file URL: "$configFile". Correct format: "s3://<bucket>/<key1>[/<key2>/...]"""")
    } else {
      val Array(bucket, key) = parts
      val s3Client = new AmazonS3Client()
      val content = s3Client.getObjectAsString(bucket, key)
      TypesafeConfigFactory.parseString(content)
    }
  }

  private def readInstanceConfigFromFile(configFile: String): TypesafeConfig = {
    val file = new File(configFile)
    val parseOptions = ConfigParseOptions.defaults().setAllowMissing(false)
    TypesafeConfigFactory.parseFile(file, parseOptions)
  }
}
