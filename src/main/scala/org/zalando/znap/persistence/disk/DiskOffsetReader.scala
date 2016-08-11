/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.persistence.disk

import java.io._

import org.apache.commons.compress.utils.IOUtils
import org.slf4j.LoggerFactory
import org.zalando.znap.config.SnapshotTarget
import org.zalando.znap.persistence.OffsetReaderSync

import scala.concurrent.{ExecutionContext, Future}

class DiskOffsetReader(override protected val snapshotTarget: SnapshotTarget)
                      (executionContext: ExecutionContext) extends OffsetReaderSync
    with DiskPersistence {

  private val logger = LoggerFactory.getLogger(classOf[DiskOffsetReader])

  private implicit val ec = executionContext

  private val partitionFilenameRegex = "partition_(?<part>\\d+)".r
  private val partitionFilenameFilter = new FilenameFilter {
    override def accept(dir: File, name: String): Boolean = {
      partitionFilenameRegex.findFirstMatchIn(name).nonEmpty
    }
  }

  override def getLastOffsets: Future[Map[String, String]] = Future {
    val partitionFiles = workingSnapshotDirectory.listFiles(partitionFilenameFilter)
    partitionFiles.map { partitionFile =>
      val lastMatch = partitionFilenameRegex.findAllMatchIn(partitionFile.getName).toList.last
      val partition = lastMatch.group("part")

      var in: BufferedReader = null
      val offset = try {
        in = new BufferedReader(new FileReader(partitionFile))
        in.readLine()
      } catch {
        case e: IOException =>
          val message = s"Error reading offset for partition $partition from file ${partitionFile.getAbsolutePath}"
          logger.error(message, e)
          throw new DiskException(message, e)
      } finally {
        IOUtils.closeQuietly(in)
      }

      partition -> offset
    }.toMap
  }
}
