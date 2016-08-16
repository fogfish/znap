/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap

package object dumps {
  type DumpUID = String

  sealed trait DumpStatus
  case object UnknownDump extends DumpStatus
  case object DumpRunning extends DumpStatus
  case object DumpFinishedSuccefully extends DumpStatus
  final case class DumpFailed(errorMessage: String) extends DumpStatus
}
