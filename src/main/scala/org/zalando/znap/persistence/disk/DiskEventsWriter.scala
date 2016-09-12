/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.persistence.disk

import java.io.{BufferedWriter, File, FileWriter, IOException}

import com.fasterxml.jackson.databind.JsonNode
import org.apache.commons.compress.utils.IOUtils
import org.slf4j.LoggerFactory
import org.zalando.znap.config.{Config, SnapshotTarget}
import org.zalando.znap.persistence.EventsWriterSync
import org.zalando.znap.utils.Json

class DiskEventsWriter(override protected val snapshotTarget: SnapshotTarget) extends EventsWriterSync
    with DiskPersistence with HashFS {
  private val logger = LoggerFactory.getLogger(classOf[DiskEventsWriter])

  override def write(events: List[JsonNode]): Unit = {
    events.foreach { e =>
      val key = Json.getKey(snapshotTarget.key, e)
      val jsonString = Json.write(e)

      // TODO compression -?

      var out: BufferedWriter = null
      try
      {
        val f = getFile(workingSnapshotDirectory, key)
        ensureDir(f)

        out = new BufferedWriter(new FileWriter(f))
        out.write(jsonString)
      } catch {
        case e: IOException =>
          val message = s"Put $key to file failed"
          logger.error(message, e)
          throw new DiskException(message, e)
      } finally {
        IOUtils.closeQuietly(out)
      }
    }
  }
}
