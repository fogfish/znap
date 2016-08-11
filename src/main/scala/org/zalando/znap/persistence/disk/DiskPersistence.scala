package org.zalando.znap.persistence.disk

import java.io.{File, IOException}

import org.slf4j.LoggerFactory
import org.zalando.znap.config.{Config, SnapshotTarget}

trait DiskPersistence {
  private val logger = LoggerFactory.getLogger(classOf[DiskPersistence])

  protected val snapshotTarget: SnapshotTarget

  protected val instanceDir = new File(
    Config.Persistence.Disk.Paths.WorkingDirectory,
    Config.ApplicationInstanceId)
  protected val workingSnapshotDirectory = new File(instanceDir, snapshotTarget.id)
  protected val snapshotsDirectory = new File(Config.Persistence.Disk.Paths.SnapshotsDirectory)
//  protected val targetSnapshotsDirectory = new File(snapshotsDirectory, snapshotTarget.id)

  def init(): Unit = {
    try {
      if (workingSnapshotDirectory.exists()) {
        logger.debug(s"Working snapshots directory ${workingSnapshotDirectory.getAbsolutePath} exists")
      } else {
        if (workingSnapshotDirectory.mkdir()) {
          logger.info(s"Working snapshot directory for target $snapshotTarget ${workingSnapshotDirectory.getAbsoluteFile} created")
        } else {
          val message = s"Can't create working snapshot directory ${workingSnapshotDirectory.getAbsoluteFile} for target $snapshotTarget"
          logger.error(message)
          throw new DiskException(message)
        }
      }

//      if (!targetSnapshotsDirectory.exists()) {
//        logger.info(s"No last snapshot for target $snapshotTarget: snapshot directory ${targetSnapshotsDirectory.getAbsoluteFile} doesn't exists")
//      } else if (targetSnapshotsDirectory.list().isEmpty) {
//        logger.info(s"No last snapshot for target $snapshotTarget: snapshot directory ${targetSnapshotsDirectory.getAbsoluteFile} is empty")
//      } else {
//        // TODO implement snapshot restoration
//        // Including snapshot consistency checks (partition files must exist, SHA-hashes, etc.)
//        ???
//      }
    } catch {
      case ex: IOException =>
        throw new DiskException(ex)
    }
  }
}
