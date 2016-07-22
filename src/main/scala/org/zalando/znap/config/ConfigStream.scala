package org.zalando.znap.config

/** Config Stream : identity of stream end-point
  *
  */
case class ConfigStream(
  /** stream protocol e.g. nakadi */
  schema: String,
  protocol: String,

  /** event stream authority */
  host: String,
  port: Int,

  /** event stream id */
  stream: String
)

object ConfigStream {

  def apply(x: String): ConfigStream = {
    val uri = new java.net.URI(x)
    val Array(schema, protocol) = uri.getScheme.split('+')
    val host = uri.getHost
    val port = uri.getPort
    val suid = uri.getPath.substring(1)

    ConfigStream(schema, protocol, host, resolvePort(schema, port), suid)
  }

  def resolvePort(schema: String, port: Int) =
    port match {
      case -1 if schema.equals("http")  => 80
      case -1 if schema.equals("https") => 443
      case _ => port
    }
}

