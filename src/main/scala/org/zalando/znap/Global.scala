package org.zalando.znap

import java.time.{LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.UUID

import com.typesafe.config.ConfigFactory

object Global {
  val ApplicationInstanceId = {
    val now = LocalDateTime.now(ZoneId.of("UTC"))
    val random = UUID.randomUUID().toString.replace("-", "").take(12)
    val format = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss")
    s"${format.format(now)}-$random"
  }

  private val config = ConfigFactory.systemProperties()
    .withFallback(ConfigFactory.defaultApplication())
  object Tokens {
    val AccessToken = config.getString("tokens.accessToken")
    val TokenInfo = config.getString("tokens.tokenInfo")
  }
}
