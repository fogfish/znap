/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.persistence.disk

import java.io._
import java.security.{DigestOutputStream, MessageDigest}
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import java.util.zip.GZIPOutputStream

import akka.actor.{Actor, ActorLogging}
import org.apache.commons.codec.binary.Hex
import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveOutputStream}
import org.apache.commons.compress.utils.IOUtils
import org.zalando.znap.config.SnapshotTarget
import org.zalando.znap.utils.NoUnexpectedMessages

class SnapshotMaker(target: SnapshotTarget, root: File, targetSnapshotsDirectory: File) extends Actor with NoUnexpectedMessages with ActorLogging {

  override def receive: Receive = {
    case SnapshotMaker.MakeSnapshotCommand =>
      makeSnapshot()
      context.parent ! SnapshotMaker.SnapshotMade
      context.stop(self)
  }

  private def makeSnapshot(): Unit = {
    log.info(s"Snapshotting for target ${target.id} started")

    val (snapshotFile, snapshotFilePart, checksumFile) = {
      val now = LocalDateTime.now(ZoneId.of("UTC"))
      val format = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss")
      val filename = s"${target.id}-${format.format(now)}.tar.gz"
      (new File(targetSnapshotsDirectory, filename),
        new File(targetSnapshotsDirectory, filename + ".part"),
        new File(targetSnapshotsDirectory, filename + ".sha1"))
    }

    if (!targetSnapshotsDirectory.exists() && !targetSnapshotsDirectory.mkdirs()) {
      throw new DiskException(s"Can't create directory ${targetSnapshotsDirectory.getAbsolutePath}")
    }

    val messageDigest = MessageDigest.getInstance("SHA-1")

    // Write .tar.gz.part file.
    var taos: TarArchiveOutputStream = null
    try {
      val fos = new FileOutputStream(snapshotFilePart)
      val sha1Stream = new DigestOutputStream(fos, messageDigest)

      val bos = new BufferedOutputStream(sha1Stream)
      val gos = new GZIPOutputStream(bos)
      taos = new TarArchiveOutputStream(gos)
      taos.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_STAR)
      taos.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU)

      root.listFiles().foreach(addFileToArchive(taos, ""))

      taos.finish()
      gos.finish()
    } catch {
      case e: IOException =>
        throw new DiskException(e)
    } finally {
      IOUtils.closeQuietly(taos)
    }

    var fw: FileWriter = null
    try {
      // Rename from .part to normal.
      snapshotFilePart.renameTo(snapshotFile)

      // Write the checksum.
      fw = new FileWriter(checksumFile)
      fw.write(Hex.encodeHexString(messageDigest.digest()))
    } catch {
      case e: IOException =>
        throw new DiskException(e)
    } finally {
      IOUtils.closeQuietly(fw)
    }

    // TODO temporary, remove
    {
      val fis1 = new FileInputStream(checksumFile)
      val sd1 = new String(IOUtils.toByteArray(fis1), "UTF-8")

      val fis2 = new FileInputStream(snapshotFile)
      messageDigest.reset()
      val sd2 = Hex.encodeHexString(
        messageDigest.digest(
          IOUtils.toByteArray(new FileInputStream(snapshotFile))
        )
      )

      //        println(sd1)
      //        println(sd2)
      assert(sd1 == sd2)

      fis1.close()
      fis2.close()
    }

    log.info(s"Snapshotting for target ${target.id} finished")
  }

  private def addFileToArchive(taos: TarArchiveOutputStream, prevPath: String)(file: File): Unit = {
    val e = prevPath + file.getName

    taos.putArchiveEntry(new TarArchiveEntry(file, e))

    if (file.isFile) {
      val bis = new BufferedInputStream(new FileInputStream(file))
      IOUtils.copy(bis, taos)
      taos.closeArchiveEntry()
      bis.close()
    } else if (file.isDirectory) {
      taos.closeArchiveEntry()
      file.listFiles().foreach(addFileToArchive(taos, e + "/"))
    }
  }
}

object SnapshotMaker {
  case object MakeSnapshotCommand
  case object SnapshotMade
}