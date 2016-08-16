package org.zalando.znap.utils

import java.io.{BufferedReader, ByteArrayInputStream, ByteArrayOutputStream, InputStreamReader}
import java.util.Base64
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import akka.http.impl.model.parser.Base64Parsing
import org.apache.commons.compress.utils.IOUtils

object Compressor {
  private val encoding = "UTF-8"

  private val base64Encoder = Base64.getEncoder
  private val base64Decoder = Base64.getDecoder

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

  def compressBase64(string: String): String = {
    base64Encoder.encodeToString(compress(string))
  }

  def decompress(array: Array[Byte]): String = {
    val bais = new ByteArrayInputStream(array)
    val gis = new GZIPInputStream(bais)
    val result = new String(IOUtils.toByteArray(gis), encoding)
    IOUtils.closeQuietly(bais)
    IOUtils.closeQuietly(gis)
    result
  }

  def decompressBase64(base64String: String): String = {
    decompress(base64Decoder.decode(base64String))
  }
}
