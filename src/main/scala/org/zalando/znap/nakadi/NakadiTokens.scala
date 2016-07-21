/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.nakadi

import java.net.URI

import org.zalando.stups.tokens.Tokens
import org.zalando.znap.Global

/**
  * For accessing Nakadi tokens.
  */
class NakadiTokens {
  private val tokens = {
    Tokens.createAccessTokensWithUri(new URI(Global.Tokens.AccessToken))
      .tokenInfoUri(new URI(Global.Tokens.TokenInfo))
      .manageToken("nakadi").addScope("uid").done()
//      .manageToken("nakadi").addScope("nakadi.event_stream.read").done()
      .start()
  }

  def get(): String = {
    tokens.get("nakadi")
  }

  def stop(): Unit = {
    tokens.stop()
  }
}
