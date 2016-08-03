/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.utils

import akka.actor.{ActorIdentity, Actor, Terminated}

/**
  * Actor that throws [[UnexpectedMessageException]] in case of unhandled (unexpected) message.
  * The only exception is unhandled Terminated, which is processed using original [[Actor.unhandled]] logic
  * ([[akka.actor.DeathPactException]]).
  */
trait NoUnexpectedMessages extends Actor {
  override final def unhandled(message: Any): Unit = {
    beforeUnhandled(message)
    message match {
      case Terminated(dead) ⇒ super.unhandled(message)
      case _: ActorIdentity ⇒ super.unhandled(message)
      case _                ⇒ throw new UnexpectedMessageException(message, sender())
    }
  }

  protected def beforeUnhandled(message: Any): Unit = {}
}
