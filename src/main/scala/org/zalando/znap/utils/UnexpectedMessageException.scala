/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.utils

import akka.actor.ActorRef

class UnexpectedMessageException(msg: Any, sender: ActorRef) extends Exception(s"Unexpected message received from $sender: $msg")
