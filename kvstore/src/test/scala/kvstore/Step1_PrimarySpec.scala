package kvstore

import akka.testkit.TestProbe

trait Step1_PrimarySpec { this: KVStoreSuite =>

  import Arbiter._

  test("Step1-case1: Primary (in isolation) should properly register itself to the provided Arbiter") {
    val arbiter = TestProbe("arbiter")
    system.actorOf(Replica.props(arbiter.ref, Persistence.props(flaky = false)),
      "step1-case1-primary")

    arbiter.expectMsg(Join)
    ()
  }

  private def step1Case2(flaky: Boolean): Unit = {
    val arbiter = TestProbe("arbiter")
    val name: String = s"step1-case2-primary${if (flaky) "-flaky" else ""}"
    val primary = system.actorOf(Replica.props(arbiter.ref,
      Persistence.props(flaky)), name)
    val client = session(primary)

    arbiter.expectMsg(Join)
    arbiter.send(primary, JoinedPrimary)

    client.getAndVerify("k1")
    client.setAcked("k1", "v1")
    client.getAndVerify("k1")
    client.getAndVerify("k2")
    client.setAcked("k2", "v2")
    client.getAndVerify("k2")
    client.removeAcked("k1")
    client.getAndVerify("k1")
  }

  test("Step1-case2: Primary (in isolation) should react properly to Insert, Remove, Get") {
    step1Case2(flaky = false)
  }

  test("Flaky Step1-case2: Primary (in isolation) should react properly to Insert, Remove, Get") {
    step1Case2(flaky = true)
  }

}
