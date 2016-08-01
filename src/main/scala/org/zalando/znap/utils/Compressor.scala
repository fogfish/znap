package org.zalando.znap.utils

import java.io.{BufferedReader, ByteArrayInputStream, ByteArrayOutputStream, InputStreamReader}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import org.apache.commons.compress.utils.IOUtils

object Compressor {
  private val encoding = "UTF-8"

  def compress(string: String): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val gos = new GZIPOutputStream(baos)
    gos.write(string.getBytes(encoding))
    gos.flush()
    baos.flush()
    IOUtils.closeQuietly(gos)
    IOUtils.closeQuietly(baos)
    baos.toByteArray
  }

  def decompress(array: Array[Byte]): String = {
    val bais = new ByteArrayInputStream(array)
    val gis = new GZIPInputStream(bais)
    val result = new String(IOUtils.toByteArray(gis), encoding)
    IOUtils.closeQuietly(bais)
    IOUtils.closeQuietly(gis)
    result
  }
}
