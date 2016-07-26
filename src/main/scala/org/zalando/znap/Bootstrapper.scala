/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap

import java.io.{File, FileFilter}

import org.slf4j.LoggerFactory
import org.zalando.znap.config.Config

/**
  * For initial directory bootstrapping.
  */
class Bootstrapper(config: Config) {
  private val logger = LoggerFactory.getLogger(classOf[Bootstrapper])

  private class SnapshotFileFilter(eventType: String) extends FileFilter {
    override def accept(pathname: File): Boolean = {
      true
    }
  }

  def bootstrap(): Unit = {
    setupWorkingDirectory()
    setupInstanceDirectory()
    setupSnapshotsDirectory()

//    Global.EventTypes.foreach(restoreSnapshot)
  }

  private def setupWorkingDirectory(): Unit = {
    val workingDir = new File(config.Persistence.Disk.Paths.WorkingDirectory)
    if (!workingDir.exists()) {
      if (workingDir.mkdir()) {
        logger.info(s"Working directory ${workingDir.getAbsoluteFile} doesn't exist, created")
      } else {
        val message = s"Can't create working directory ${workingDir.getAbsoluteFile}"
        logger.error(message)
        throw new Exception(message)
      }
    } else {
      logger.debug(s"Working directory ${workingDir.getAbsoluteFile} exists")
    }
  }

  private def setupInstanceDirectory(): Unit = {
    val instanceDir = new File(config.Persistence.Disk.Paths.WorkingDirectory, config.ApplicationInstanceId)
    if (instanceDir.exists()) {
      val message = s"Instance directory ${instanceDir.getAbsoluteFile} exists"
      logger.error(message)
      throw new Exception(message)
    } else {
      if (instanceDir.mkdir()) {
        logger.info(s"Instance directory ${instanceDir.getAbsoluteFile} created")
      } else {
        val message = s"Can't create instance directory ${instanceDir.getAbsoluteFile}"
        logger.error(message)
        throw new Exception(message)
      }
    }
  }

  private def setupSnapshotsDirectory(): Unit = {
    val snapshotsDirectory = new File(config.Persistence.Disk.Paths.SnapshotsDirectory)
    if (!snapshotsDirectory.exists()) {
      if (snapshotsDirectory.mkdir()) {
        logger.info(s"Snapshots directory ${snapshotsDirectory.getAbsoluteFile} doesn't exist, created")
      } else {
        val message = s"Can't create snapshots directory ${snapshotsDirectory.getAbsoluteFile}"
        logger.error(message)
        throw new Exception(message)
      }
    } else {
      logger.debug(s"Snapshots directory ${snapshotsDirectory.getAbsoluteFile} exists")
    }
  }

  private def restoreSnapshot(eventType: String): Unit = {
//    logger.info(s"Trying to restore last snapshot for event type $eventType")
//
//    val instanceDir = new File(Global.Paths.WorkingDirectory, Global.ApplicationInstanceId)
//    val workingSnapshotDirectory = new File(instanceDir, eventType)
//    if (workingSnapshotDirectory.mkdir()) {
//      logger.info(s"Working snapshot directory for event type $eventType ${workingSnapshotDirectory.getAbsoluteFile} created")
//    } else {
//      val message = s"Can't create working snapshot directory ${workingSnapshotDirectory.getAbsoluteFile}"
//      logger.error(message)
//      throw new Exception(message)
//    }
//
//    val snapshotDirectory = new File(Global.Paths.SnapshotsDirectory, eventType)
//    if (!snapshotDirectory.exists()) {
//      logger.info(s"No last snapshot for event type $eventType: snapshot directory ${snapshotDirectory.getAbsoluteFile} doesn't exists")
//    } else {
//      ???
//    }
  }
}
