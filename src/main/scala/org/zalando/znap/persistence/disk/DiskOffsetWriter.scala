package org.zalando.znap.persistence.disk

import java.io.{BufferedWriter, File, FileWriter, IOException}

import org.apache.commons.compress.utils.IOUtils
import org.slf4j.LoggerFactory
import org.zalando.znap.config.{Config, SnapshotTarget}
import org.zalando.znap.source.nakadi.objects.Cursor
import org.zalando.znap.persistence.OffsetWriterSync

import scala.concurrent.{ExecutionContext, Future}

class DiskOffsetWriter(override protected val snapshotTarget: SnapshotTarget) extends OffsetWriterSync
    with DiskPersistence {
  private val logger = LoggerFactory.getLogger(classOf[DiskOffsetWriter])

  override def write(cursor: Cursor): Unit = {
    val partitionFile = new File(workingSnapshotDirectory, s"partition_${cursor.partition}")

    var out: BufferedWriter = null
    try
    {
      partitionFile.createNewFile()
      out = new BufferedWriter(new FileWriter(partitionFile))
      out.write(cursor.offset)
    } catch {
      case e: IOException =>
        val message = s"Error writing offset ${cursor.offset} for partition ${cursor.partition}"
        logger.error(message, e)
        throw new DiskException(message, e)
    } finally {
      IOUtils.closeQuietly(out)
    }
  }
}
