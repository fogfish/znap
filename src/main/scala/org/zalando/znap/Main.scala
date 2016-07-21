package org.zalando.znap

import org.zalando.znap.nakadi.NakadiTokens
import org.slf4j.LoggerFactory

object Main extends App {
  val logger = LoggerFactory.getLogger(Main.getClass)
  logger.info(s"Application instance started with ID ${Global.ApplicationInstanceId}")

  val tokens = new NakadiTokens

  tokens.stop()
}
