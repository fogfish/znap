/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.dumps

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import org.scalatest.{FunSpec, Matchers}
import org.zalando.znap.config.{EmptyDestination, EmptySource, SnapshotTarget}
import org.zalando.znap.dumps

class DumpTrackerSuite extends FunSpec with Matchers {

  private val actorSystem = ActorSystem()
  private val dumpRunner1 = actorSystem.actorOf(Props.empty)
  private val dumpRunner2 = actorSystem.actorOf(Props.empty)
  private val dumpUid1 = "x1"
  private val dumpUid2 = "x2"

  private val target1 = SnapshotTarget(
    "id1", EmptySource, EmptyDestination, None, None, Nil, compress = false)
  private val target2 = SnapshotTarget(
    "id2", EmptySource, EmptyDestination, None, None, Nil, compress = false)

  it("should not return status for an unknown dump") {
    val dt = new DumpTracker
    dt.getStatus("unknown") shouldBe dumps.UnknownDump
  }

  it("should return status for a started dump") {
    val dt = new DumpTracker
    val dumpUid = "x"
    dt.dumpStarted(target1, dumpUid, ActorRef.noSender)
    dt.getStatus(dumpUid) should not be dumps.UnknownDump
  }

  it("should finish a dump with a legal runner") {
    val dt = new DumpTracker
    val dumpUid = "x"

    dt.dumpStarted(target1, dumpUid, dumpRunner1)
    intercept[IllegalStateException] {
      dt.dumpFinishedSuccessfully(dumpRunner2)
    }
    dt.dumpFinishedSuccessfully(dumpRunner1) shouldBe dumpUid
  }

  it("should fail a dump with a legal runner") {
    val dt = new DumpTracker
    dt.dumpStarted(target1, dumpUid1, dumpRunner1)
    intercept[IllegalStateException] {
      dt.dumpFailed(dumpRunner2, "message")
    }
    dt.dumpFailed(dumpRunner1, "message") shouldBe dumpUid1
  }

  it("should return status for a finished dump") {
    val dt = new DumpTracker
    dt.dumpStarted(target1, dumpUid1, dumpRunner1)
    dt.dumpFinishedSuccessfully(dumpRunner1) shouldBe dumpUid1
    dt.getStatus(dumpUid1) shouldBe dumps.DumpFinishedSuccefully
  }

  it("should return status for a failed dump") {
    val dt = new DumpTracker
    val message = "message"
    dt.dumpStarted(target1, dumpUid1, dumpRunner1)
    dt.dumpFailed(dumpRunner1, message) shouldBe dumpUid1
    dt.getStatus(dumpUid1) shouldBe dumps.DumpFailed(message)
  }

  it("should not allow to finish a not started dump") {
    val dt = new DumpTracker
    intercept[IllegalStateException] {
      dt.dumpFinishedSuccessfully(dumpRunner1)
    }
  }

  it("should not allow to fail a not started dump") {
    val dt = new DumpTracker
    intercept[IllegalStateException] {
      dt.dumpFailed(dumpRunner1, "message")
    }
  }

  it("should not allow to finish a failed dump") {
    val dt = new DumpTracker
    dt.dumpStarted(target1, dumpUid1, dumpRunner1)
    dt.dumpFailed(dumpRunner1, "message") shouldBe dumpUid1

    intercept[IllegalStateException] {
      dt.dumpFinishedSuccessfully(dumpRunner1)
    }
  }

  it("should not allow to finish a finished dump") {
    val dt = new DumpTracker
    dt.dumpStarted(target1, dumpUid1, dumpRunner1)
    dt.dumpFinishedSuccessfully(dumpRunner1) shouldBe dumpUid1

    intercept[IllegalStateException] {
      dt.dumpFinishedSuccessfully(dumpRunner1)
    }
  }

  it("should not allow to fail a finished dump") {
    val dt = new DumpTracker
    dt.dumpStarted(target1, dumpUid1, dumpRunner1)
    dt.dumpFinishedSuccessfully(dumpRunner1) shouldBe dumpUid1

    intercept[IllegalStateException] {
      dt.dumpFailed(dumpRunner1, "message")
    }
  }

  it("should not allow to fail a failed dump") {
    val dt = new DumpTracker
    dt.dumpStarted(target1, dumpUid1, dumpRunner1)
    dt.dumpFailed(dumpRunner1, "message") shouldBe dumpUid1

    intercept[IllegalStateException] {
      dt.dumpFailed(dumpRunner1, "message")
    }
  }

  it("should not allow to start two dumps with one target") {
    val dt = new DumpTracker
    dt.dumpStarted(target1, dumpUid1, dumpRunner1)
    intercept[IllegalStateException] {
      dt.dumpStarted(target1, dumpUid2, dumpRunner2)
    }
  }

  it("should not allow to start two dumps with one runner") {
    val dt = new DumpTracker
    dt.dumpStarted(target1, dumpUid1, dumpRunner1)
    intercept[IllegalStateException] {
      dt.dumpStarted(target2, dumpUid2, dumpRunner1)
    }
  }

  it("should not allow to start two dumps with one uid") {
    val dt = new DumpTracker
    dt.dumpStarted(target1, dumpUid1, dumpRunner1)
    intercept[IllegalStateException] {
      dt.dumpStarted(target2, dumpUid1, dumpRunner2)
    }
  }

  it("should not allow to start a dump with the uid of some finished dump") {
    val dt = new DumpTracker
    dt.dumpStarted(target1, dumpUid1, dumpRunner1)
    dt.dumpFinishedSuccessfully(dumpRunner1) shouldBe dumpUid1

    intercept[IllegalStateException] {
      dt.dumpStarted(target2, dumpUid1, dumpRunner1)
    }
  }

  it("should not allow to start a dump with the uid of some failed dump") {
    val dt = new DumpTracker
    dt.dumpStarted(target1, dumpUid1, dumpRunner1)
    dt.dumpFailed(dumpRunner1, "message") shouldBe dumpUid1

    intercept[IllegalStateException] {
      dt.dumpStarted(target2, dumpUid1, dumpRunner1)
    }
  }
}
