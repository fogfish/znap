/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.persistence.disk

import java.io.File

trait HashFS {
  import HashFS._

  protected def getFile(root: File, id: String): File = {
    val key = hash.digest(id.getBytes("UTF-8"))
      .map("%02x".format(_))
      .mkString

    val dir = new File(root, key.substring(0, 2))
    new File(dir, id)
  }

  protected def ensureDir(file: File) = {
    if (!file.getParentFile.exists())
      file.getParentFile.mkdirs()
  }
}

object HashFS {
  private val hash = java.security.MessageDigest.getInstance("SHA-1")
}