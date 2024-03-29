package kvstore

import akka.testkit.TestProbe
import kvstore.Arbiter._
import kvstore.Persistence._
import kvstore.Replicator._

import scala.concurrent.duration._

trait Step5_PrimaryPersistenceSpec { this: KVStoreSuite =>

  test("Step5-case1: Primary does not acknowledge updates which have not been persisted") {
    val arbiter = TestProbe("arbiter")
    val persistence = TestProbe("persistence")
    val primary = system.actorOf(Replica.props(arbiter.ref,
      probeProps(persistence)), "step5-case1-primary")
    val client = session(primary)

    arbiter.expectMsg(Join)
    arbiter.send(primary, JoinedPrimary)

    val setId = client.set("foo", "bar")
    val persistId = persistence.expectMsgPF() {
      case Persist("foo", Some("bar"), id) => id
    }

    client.nothingHappens(100.milliseconds)
    persistence.reply(Persisted("foo", persistId))
    client.waitAck(setId)
  }

  test("Step5-case2: Primary retries persistence every 100 milliseconds") {
    val arbiter = TestProbe("arbiter")
    val persistence = TestProbe("persistence")
    val primary = system.actorOf(Replica.props(arbiter.ref,
      probeProps(persistence)), "step5-case2-primary")
    val client = session(primary)

    arbiter.expectMsg(Join)
    arbiter.send(primary, JoinedPrimary)

    val setId = client.set("foo", "bar")
    val persistId = persistence.expectMsgPF() {
      case Persist("foo", Some("bar"), id) => id
    }
    // Retries form above
    persistence.expectMsg(200.milliseconds, Persist("foo", Some("bar"), persistId))
    persistence.expectMsg(200.milliseconds, Persist("foo", Some("bar"), persistId))

    client.nothingHappens(100.milliseconds)
    persistence.reply(Persisted("foo", persistId))
    client.waitAck(setId)
  }

  test("Step5-case3: Primary generates failure after 1 second if persistence fails") {
    val arbiter = TestProbe("arbiter")
    val persistence = TestProbe("persistence")
    val primary = system.actorOf(Replica.props(arbiter.ref,
      probeProps(persistence)), "step5-case3-primary")
    val client = session(primary)

    arbiter.expectMsg(Join)
    arbiter.send(primary, JoinedPrimary)

    val setId = client.set("foo", "bar")
    persistence.expectMsgType[Persist]
    client.nothingHappens(800.milliseconds) // Should not fail too early
    client.waitFailed(setId)
  }

  private def step5Case4(flaky: Boolean): Unit = {
    val arbiter = TestProbe("arbiter")
    val name = s"step5-case4-primary${if (flaky) "-flaky" else ""}"
    val primary = system.actorOf(Replica.props(arbiter.ref,
      Persistence.props(flaky)), name)
    val secondary = TestProbe("secondary")
    val client = session(primary)

    arbiter.expectMsg(Join)
    arbiter.send(primary, JoinedPrimary)
    arbiter.send(primary, Replicas(Set(primary, secondary.ref)))

    client.probe.within(1.second, 2.seconds) {
      val setId = client.set("foo", "bar")
      secondary.expectMsgType[Snapshot](200.millis)
      client.waitFailed(setId)
    }
  }

  test("Step5-case4: Primary generates failure after 1 second if global acknowledgement fails") {
    step5Case4(flaky = false)
  }
  test("Flaky Step5-case4: Primary generates failure after 1 second if global acknowledgement fails") {
    step5Case4(flaky = true)
  }

  private def step5Case5(flaky: Boolean): Unit = {
    val arbiter = TestProbe("arbiter")
    val name = s"step5-case5-primary${if (flaky) "-flaky" else ""}"
    val primary = system.actorOf(Replica.props(arbiter.ref,
      Persistence.props(flaky)), name)
    val secondaryA = TestProbe("secondaryA")
    val secondaryB = TestProbe("secondaryB")
    val client = session(primary)

    arbiter.expectMsg(Join)
    arbiter.send(primary, JoinedPrimary)
    arbiter.send(primary, Replicas(Set(primary, secondaryA.ref, secondaryB.ref)))

    val setId = client.set("foo", "bar")
    val seqA = secondaryA.expectMsgType[Snapshot].seq
    val seqB = secondaryB.expectMsgType[Snapshot].seq
    client.nothingHappens(300.milliseconds)
    secondaryA.reply(SnapshotAck("foo", seqA))
    client.nothingHappens(300.milliseconds)
    secondaryB.reply(SnapshotAck("foo", seqB))
    client.waitAck(setId)
  }

  test("Step5-case5: Primary acknowledges only after persistence and global acknowledgement") {
    step5Case5(flaky = false)
  }
  test("Flaky Step5-case5: Primary acknowledges only after persistence and global acknowledgement") {
    step5Case5(flaky = true)
  }

}
