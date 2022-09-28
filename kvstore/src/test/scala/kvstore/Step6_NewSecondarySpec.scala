package kvstore

import akka.testkit.TestProbe
import kvstore.Arbiter._
import kvstore.Replicator._

trait Step6_NewSecondarySpec { this: KVStoreSuite =>

  private def step6Case1(flaky: Boolean): Unit = {
    val arbiter = TestProbe("arbiter")
    val name = s"step6-case1-primary${if (flaky) "-flaky" else ""}"
    val primary = system.actorOf(Replica.props(arbiter.ref,
      Persistence.props(flaky)), name)
    val user = session(primary)
    val secondary = TestProbe("secondary")

    arbiter.expectMsg(Join)
    arbiter.send(primary, JoinedPrimary)

    user.setAcked("k1", "v1")
    arbiter.send(primary, Replicas(Set(primary, secondary.ref)))

    expectAtLeastOneSnapshot(secondary)("k1", Some("v1"), 0L)

    val ack1 = user.set("k1", "v2")
    expectAtLeastOneSnapshot(secondary)("k1", Some("v2"), 1L)
    user.waitAck(ack1)

    val ack2 = user.remove("k1")
    expectAtLeastOneSnapshot(secondary)("k1", None, 2L)
    user.waitAck(ack2)
  }

  test("Step6-case1: Primary must start replication to new replicas") {
    step6Case1(flaky = false)
  }
  test("Flaky Step6-case1: Primary must start replication to new replicas") {
    step6Case1(flaky = true)
  }

  private def step6Case2(flaky: Boolean): Unit = {
    val probe = TestProbe("probe")
    val arbiter = TestProbe("arbiter")
    val name = s"step6-case2-primary${if (flaky) "-flaky" else ""}"
    val primary = system.actorOf(Replica.props(arbiter.ref,
      Persistence.props(flaky)), name)
    val user = session(primary)
    val secondary = TestProbe("secondary")

    arbiter.expectMsg(Join)
    arbiter.send(primary, JoinedPrimary)
    arbiter.send(primary, Replicas(Set(primary, secondary.ref)))

    val ack1 = user.set("k1", "v1")
    secondary.expectMsg(Snapshot("k1", Some("v1"), 0L))
    val replicator = secondary.lastSender
    secondary.reply(SnapshotAck("k1", 0L))
    user.waitAck(ack1)

    probe.watch(replicator)
    arbiter.send(primary, Replicas(Set(primary)))
    probe.expectTerminated(replicator)
    ()
  }

  test("Step6-case2: Primary must stop replication to removed replicas and stop Replicator") {
    step6Case2(flaky = false)
  }
  test("Flaky Step6-case2: Primary must stop replication to removed replicas and stop Replicator") {
    step6Case2(flaky = true)
  }

  private def step6Case3(flaky: Boolean): Unit = {
    val arbiter = TestProbe("arbiter")
    val name = s"step6-case3-primary${if (flaky) "-flaky" else ""}"
    val primary = system.actorOf(Replica.props(arbiter.ref,
      Persistence.props(flaky)), name)
    val user = session(primary)
    val secondary = TestProbe("secondary")

    arbiter.expectMsg(Join)
    arbiter.send(primary, JoinedPrimary)
    arbiter.send(primary, Replicas(Set(primary, secondary.ref)))

    val ack1 = user.set("k1", "v1")
    secondary.expectMsg(Snapshot("k1", Some("v1"), 0L))
    secondary.reply(SnapshotAck("k1", 0L))
    user.waitAck(ack1)

    val ack2 = user.set("k1", "v2")
    secondary.expectMsg(Snapshot("k1", Some("v2"), 1L))
    arbiter.send(primary, Replicas(Set(primary)))
    user.waitAck(ack2)
  }

  test("Step6-case3: Primary must stop replication to removed replicas and waive their outstanding " +
    "acknowledgements") {
    step6Case3(flaky = false)
  }
  test("Flaky Step6-case3: Primary must stop replication to removed replicas and waive their outstanding " +
    "acknowledgements") {
    step6Case3(flaky = true)
  }

}
