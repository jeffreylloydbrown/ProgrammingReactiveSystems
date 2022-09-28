package kvstore

import akka.testkit.TestProbe
import kvstore.Arbiter.{Join, JoinedSecondary}

import scala.concurrent.duration._

trait Step2_SecondarySpec { this: KVStoreSuite =>

  private def step2Case1(flaky: Boolean): Unit = {
    val arbiter = TestProbe("arbiter")
    val name = s"step2-case1-secondary${if (flaky) "-flaky" else ""}"
    system.actorOf(Replica.props(arbiter.ref, Persistence.props(flaky)),
      name)

    arbiter.expectMsg(Join)
    ()
  }

  test("Step2-case1: Secondary (in isolation) should properly register itself to the provided Arbiter") {
    step2Case1(flaky = false)
  }
  test("Flaky Step2-case1: Secondary (in isolation) should properly register itself to the provided Arbiter") {
    step2Case1(flaky = true)
  }

  private def step2Case2(flaky: Boolean): Unit = {
    import Replicator._

    val arbiter = TestProbe("arbiter")
    val replicator = TestProbe("replicator")
    val name = s"step2-case2-secondary${if (flaky) "-flaky" else ""}"
    val secondary = system.actorOf(Replica.props(arbiter.ref,
      Persistence.props(flaky)), name)
    val client = session(secondary)

    arbiter.expectMsg(Join)
    arbiter.send(secondary, JoinedSecondary)

    assertEquals(client.get("k1"), None)

    replicator.send(secondary, Snapshot("k1", None, 0L))
    replicator.expectMsg(SnapshotAck("k1", 0L))
    assertEquals(client.get("k1"), None)

    replicator.send(secondary, Snapshot("k1", Some("v1"), 1L))
    replicator.expectMsg(SnapshotAck("k1", 1L))
    assertEquals(client.get("k1"), Some("v1"))

    replicator.send(secondary, Snapshot("k1", None, 2L))
    replicator.expectMsg(SnapshotAck("k1", 2L))
    assertEquals(client.get("k1"), None)
  }

  test("Step2-case2: Secondary (in isolation) must handle Snapshots") {
    step2Case2(flaky = false)
  }
  test("Flaky Step2-case2: Secondary (in isolation) must handle Snapshots") {
    step2Case2(flaky = true)
  }

  private def step2Case3(flaky: Boolean): Unit = {
    import Replicator._

    val arbiter = TestProbe("arbiter")
    val replicator = TestProbe("replicator")
    val name = s"step2-case3-secondary${if (flaky) "-flaky" else ""}"
    val secondary = system.actorOf(Replica.props(arbiter.ref,
      Persistence.props(flaky)), name)
    val client = session(secondary)

    arbiter.expectMsg(Join)
    arbiter.send(secondary, JoinedSecondary)

    assertEquals(client.get("k1"), None)

    replicator.send(secondary, Snapshot("k1", Some("v1"), 0L))
    replicator.expectMsg(SnapshotAck("k1", 0L))
    assertEquals(client.get("k1"), Some("v1"))

    replicator.send(secondary, Snapshot("k1", None, 0L))
    replicator.expectMsg(SnapshotAck("k1", 0L))
    assertEquals(client.get("k1"), Some("v1"))

    replicator.send(secondary, Snapshot("k1", Some("v2"), 1L))
    replicator.expectMsg(SnapshotAck("k1", 1L))
    assertEquals(client.get("k1"), Some("v2"))

    replicator.send(secondary, Snapshot("k1", None, 0L))
    replicator.expectMsg(SnapshotAck("k1", 0L))
    assertEquals(client.get("k1"), Some("v2"))
  }

  test("Step2-case3: Secondary should drop and immediately ack snapshots with older sequence numbers") {
    step2Case3(flaky = false)
  }
  test("Flaky Step2-case3: Secondary should drop and immediately ack snapshots with older sequence numbers") {
    step2Case3(flaky = true)
  }

  private def step2Case4(flaky: Boolean): Unit = {
    import Replicator._

    val arbiter = TestProbe("arbiter")
    val replicator = TestProbe("arbiter")
    val name = s"step2-case4-secondary${if (flaky) "-flaky" else ""}"
    val secondary = system.actorOf(Replica.props(arbiter.ref,
      Persistence.props(flaky)), name)
    val client = session(secondary)

    arbiter.expectMsg(Join)
    arbiter.send(secondary, JoinedSecondary)

    assertEquals(client.get("k1"), None)

    replicator.send(secondary, Snapshot("k1", Some("v1"), 1L))
    replicator.expectNoMessage(300.milliseconds)
    assertEquals(client.get("k1"), None)

    replicator.send(secondary, Snapshot("k1", Some("v2"), 0L))
    replicator.expectMsg(SnapshotAck("k1", 0L))
    assertEquals(client.get("k1"), Some("v2"))
  }

  test("Step2-case4: Secondary should drop snapshots with future sequence numbers") {
    step2Case4(flaky = false)
  }
  test("Flaky Step2-case4: Secondary should drop snapshots with future sequence numbers") {
    step2Case4(flaky = true)
  }

}
