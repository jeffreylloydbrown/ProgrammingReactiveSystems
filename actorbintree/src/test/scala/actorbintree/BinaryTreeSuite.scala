/**
 * Copyright (C) 2009-2015 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor.{ActorRef, ActorSystem, Props, actorRef2Scala}
import akka.testkit.{ImplicitSender, TestKitBase, TestProbe}

import scala.concurrent.duration._
import scala.util.Random

class BinaryTreeSuite extends munit.FunSuite with TestKitBase with ImplicitSender {
  implicit lazy val system: ActorSystem = ActorSystem("BinaryTreeSuite")

  import actorbintree.BinaryTreeSet._

  def receiveN(requester: TestProbe, ops: Seq[Operation], expectedReplies: Seq[OperationReply]): Unit =
    requester.within(5.seconds) {
      val repliesUnsorted = for (i <- 1 to ops.size) yield try {
        requester.expectMsgType[OperationReply]
      } catch {
        case ex: Throwable if ops.size > 10 => sys.error(s"failure to receive confirmation $i/${ops.size}\n$ex")
        case ex: Throwable                  => sys.error(s"failure to receive confirmation $i/${ops.size}\nRequests:" +
          ops.mkString("\n    ", "\n     ", "") + s"\n$ex")
      }
      val replies = repliesUnsorted.sortBy(_.id)
      if (replies != expectedReplies) {
        val pairs = (replies zip expectedReplies).zipWithIndex
          .filter(x => x._1._1 != x._1._2)
        fail("unexpected replies:" + pairs.map(x => s"at index ${x._2}: got ${x._1._1}, expected ${x._1._2}")
          .mkString("\n    ", "\n    ", ""))
      }
    }

  def verify(probe: TestProbe, ops: Seq[Operation], expected: Seq[OperationReply]): Unit = {
    val topNode = system.actorOf(Props[BinaryTreeSet]())

    ops foreach { op =>
      topNode ! op
    }

    receiveN(probe, ops, expected)
    // the grader also verifies that enough actors are created
  }

  test("new BinaryTreeSet reads as empty (even though it has a deleted node)") {
    val topNode = system.actorOf(Props[BinaryTreeSet]())

    topNode ! Contains(testActor, id = 1, 0)
    expectMsg(ContainsResult(1, result = false))
  }

  test("inserting the 'removed' element of a new BinaryTreeSet makes it found") {
    val topNode = system.actorOf(Props[BinaryTreeSet]())

    topNode ! Insert(testActor, id = 1, 0)
    expectMsg(OperationFinished(id = 1))

    topNode ! Contains(testActor, id = 2, 0)
    expectMsg(ContainsResult(id = 2, result = true))
  }

  test("inserting 1 and -1 get found, zero not found cuz not inserted") {
    val topNode = system.actorOf(Props[BinaryTreeSet]())

    topNode ! Insert(testActor, id = 1, elem = 1)
    expectMsg(OperationFinished(id = 1))

    topNode ! Insert(testActor, id = 2, elem = -1)
    expectMsg(OperationFinished(id = 2))

    topNode ! Contains(testActor, id = 3, elem = 1)
    expectMsg(ContainsResult(id = 3, result = true))

    topNode ! Contains(testActor, id = 4, elem = -1)
    expectMsg(ContainsResult(id = 4, result = true))

    topNode ! Contains(testActor, id = 5, elem = 0)
    expectMsg(ContainsResult(id = 5, result = false))
  }

  test("insert -2, -1, 2, 1 should all be found with 0 not found") {
    val topNode = system.actorOf(Props[BinaryTreeSet]())
    val data = List((1, -2), (3, -1), (5, 2), (7, 1))
    data.foreach { case (id, value) =>
      topNode ! Insert(testActor, id, value)
      expectMsg(OperationFinished(id))
    }
    data.foreach { case (id, value) =>
      topNode ! Contains(testActor, id+1, value)
      expectMsg(ContainsResult(id+1, result = true))
    }

    val zeroId = 424242 // some unused id, doesn't matter.
    topNode ! Contains(testActor, zeroId, 0)
    expectMsg(ContainsResult(zeroId, result = false))
  }

  test("insert same element twice it should be found") {
    val topNode = system.actorOf(Props[BinaryTreeSet]())
    val elem = 19
    topNode ! Insert(testActor, 12, elem)
    expectMsg(OperationFinished(12))
    topNode ! Insert(testActor, 13, elem)
    expectMsg(OperationFinished(13))
    topNode ! Contains(testActor, 14, elem)
    expectMsg(ContainsResult(14, result = true))
  }

  test("insert 1, search for 2 returns not found") {
    val topNode = system.actorOf(Props[BinaryTreeSet]())
    topNode ! Insert(testActor, 1, 1)
    expectMsg(OperationFinished(1))
    topNode ! Contains(testActor, 27, 2)
    expectMsg(ContainsResult(27, result = false))
  }

  test("removing the root node from an empty BinaryTreeSet completes ok") {
    val topNode = system.actorOf(Props[BinaryTreeSet]())
    topNode ! Remove(testActor, 1, defaultRootElement)
    expectMsg(OperationFinished(1))
  }

  test("removing an element not in a non-empty BinaryTreeSet is ok") {
    val topNode = system.actorOf(Props[BinaryTreeSet]())
    val target = 17
    val otherElem = target + 2
    topNode ! Insert(testActor, 1, otherElem)
    expectMsg(OperationFinished(1))
    topNode ! Contains(testActor, 2, otherElem)
    expectMsg(ContainsResult(2, result = true))
    topNode ! Contains(testActor, 3, target)
    expectMsg(ContainsResult(3, result = false))  // just confirming not present before removing it.
    topNode ! Remove(testActor, 4, target)
    expectMsg(OperationFinished(4))
    topNode ! Contains(testActor, 5, otherElem)
    expectMsg(ContainsResult(5, result = true))
    topNode ! Contains(testActor, 6, target)
    expectMsg(ContainsResult(6, result = false))
  }

  test("add a node to an empty BinaryTreeSet, remove it, node not found") {
    val topNode = system.actorOf(Props[BinaryTreeSet]())
    val elem = 91
    topNode ! Insert(testActor, 1, elem)
    expectMsg(OperationFinished(1))
    topNode ! Remove(testActor, 2, elem)
    expectMsg(OperationFinished(2))
    topNode ! Contains(testActor, 3, elem)
    expectMsg(ContainsResult(3, result = false))
  }

  test("add a leaf node to a non-empty BinaryTreeSet, remove it, node not found") {
    val topNode = system.actorOf(Props[BinaryTreeSet]())
    val elem = 49
    topNode ! Insert(testActor, 1, 1) // to make tree not empty
    expectMsg(OperationFinished(1))
    topNode ! Insert( testActor, 2, elem)
    expectMsg(OperationFinished(2))
    topNode ! Contains(testActor, 3, elem)
    expectMsg(ContainsResult(3, result = true))
    topNode ! Remove(testActor, 4, elem)
    expectMsg(OperationFinished(4))
    topNode ! Contains(testActor, 5, elem)
    expectMsg(ContainsResult(5, result = false))
  }

  test("add an interior node to a non-empty BinaryTreeSet, remove it, node not found but other node still found") {
    val topNode = system.actorOf(Props[BinaryTreeSet]())
    val makeItNotEmptyElem = 29
    val removeTargetElem = 23

    topNode ! Insert(testActor, 1, makeItNotEmptyElem)
    expectMsg(OperationFinished(1))
    topNode ! Insert(testActor, 2, removeTargetElem)
    expectMsg(OperationFinished(2))
    topNode ! Contains(testActor, 3, makeItNotEmptyElem)
    expectMsg(ContainsResult(3, result = true))
    topNode ! Contains(testActor, 4, removeTargetElem)
    expectMsg(ContainsResult(4, result = true))
    // now remove the target element, then confirm it isn't found while the other element is still found.
    topNode ! Remove(testActor, 5, removeTargetElem)
    expectMsg(OperationFinished(5))
    topNode ! Contains(testActor, 6, removeTargetElem)
    expectMsg(ContainsResult(6, result = false))
    topNode ! Contains(testActor, 7, makeItNotEmptyElem)
    expectMsg(ContainsResult(7, result = true))
  }

  test("proper inserts and lookups (5pts)") {
    val topNode = system.actorOf(Props[BinaryTreeSet]())

    topNode ! Contains(testActor, id = 1, 1)
    expectMsg(ContainsResult(1, result = false))

    topNode ! Insert(testActor, id = 2, 1)
    expectMsg(OperationFinished(2))

    topNode ! Contains(testActor, id = 3, 1)
    expectMsg(ContainsResult(3, result = true))
  }

  test("instruction example (5pts)") {
    val requester = TestProbe()
    val requesterRef = requester.ref
    val ops = List(
      Insert(requesterRef, id=100, 1),
      Contains(requesterRef, id=50, 2),
      Remove(requesterRef, id=10, 1),
      Insert(requesterRef, id=20, 2),
      Contains(requesterRef, id=80, 1),
      Contains(requesterRef, id=70, 2)
    )

    val expectedReplies = List(
      OperationFinished(id=10),
      OperationFinished(id=20),
      ContainsResult(id=50, result = false),
      ContainsResult(id=70, result = true),
      ContainsResult(id=80, result = false),
      OperationFinished(id=100)
    )

    verify(requester, ops, expectedReplies)
  }

  test("behave identically to built-in set (includes GC) (40pts)") {
    val rnd = new Random()
    def randomOperations(requester: ActorRef, count: Int): Seq[Operation] = {
      def randomElement: Int = rnd.nextInt(100)
      def randomOperation(requester: ActorRef, id: Int): Operation = rnd.nextInt(4) match {
        case 0 => Insert(requester, id, randomElement)
        case 1 => Insert(requester, id, randomElement)
        case 2 => Contains(requester, id, randomElement)
        case 3 => Remove(requester, id, randomElement)
      }

      for (seq <- 0 until count) yield randomOperation(requester, seq)
    }

    def referenceReplies(operations: Seq[Operation]): Seq[OperationReply] = {
      var referenceSet = Set.empty[Int]
      def replyFor(op: Operation): OperationReply = op match {
        case Insert(_, seq, elem) =>
          referenceSet = referenceSet + elem
          OperationFinished(seq)
        case Remove(_, seq, elem) =>
          referenceSet = referenceSet - elem
          OperationFinished(seq)
        case Contains(_, seq, elem) =>
          ContainsResult(seq, referenceSet(elem))
      }

      for (op <- operations) yield replyFor(op)
    }

    val requester = TestProbe()
    val topNode = system.actorOf(Props[BinaryTreeSet]())
    val count = 1000

    val ops = randomOperations(requester.ref, count)
    val expectedReplies = referenceReplies(ops)

    ops foreach { op =>
      topNode ! op
      if (rnd.nextDouble() < 0.1) topNode ! GC
    }
    receiveN(requester, ops, expectedReplies)
  }
}
