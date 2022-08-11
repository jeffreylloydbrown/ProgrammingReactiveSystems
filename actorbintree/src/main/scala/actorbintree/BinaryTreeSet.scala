/**
  * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
  */
package actorbintree

import akka.actor._
import akka.event.LoggingReceive

import scala.collection.immutable.Queue

object BinaryTreeSet {

  sealed trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation
  object Insert {
    val Name = "Insert"
  }

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation
  object Remove {
    val Name = "Remove"
  }

  /** Request to perform garbage collection */
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

  /** This is the element inserted into a new BinaryTreeSet when the
    * BinaryTreeSet is created.
    */
  val defaultRootElement = 0

}


class BinaryTreeSet extends Actor with ActorLogging {
  import BinaryTreeSet._

  def createRoot: ActorRef = context.actorOf(Props(classOf[BinaryTreeNode], defaultRootElement, true),
    s"root-elem-$defaultRootElement")

  var root: ActorRef = createRoot

  // optional (used to stash incoming operations during garbage collection)
  var pendingQueue: Queue[Operation] = Queue.empty[Operation]

  // optional
  def receive: Receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = LoggingReceive {
    case Contains(requester, id, elem) =>
      log.debug("forwarding Contains({}, {}, {}) to {}", requester, id, elem, root)
      root ! Contains(requester, id, elem)

    case Insert(requester, id, elem) =>
      log.debug("forwarding Insert({}, {}, {}) to {}", requester, id, elem, root)
      root ! Insert(requester, id, elem)

    case Remove(requester, id, elem) =>
      log.debug("forwarding Remove({}, {}, {}) to {}", requester, id, elem, root)
      root ! Remove(requester, id, elem)
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = ???

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  /**
    * Acknowledges that a copy has been completed. This message should be sent
    * from a node to its parent, when this node and all its children nodes have
    * finished being copied.
    */
  case object CopyFinished

}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor with ActorLogging {
  import BinaryTreeNode._
  import BinaryTreeSet._

  def receive: Receive = normal(Map[Position, ActorRef](), initiallyRemoved)

  private def searchSubTree(requester: ActorRef,
                            subtrees: Map[Position, ActorRef],
                            p: Position, id: Int, searchingForElem: Int): Unit =
    subtrees.get(p) match {
      case Some(subtree) =>
        val c = Contains(requester, id, searchingForElem)
        log.debug("searchSubTree-{}: forwarding {} to {}", p, c, subtree)
        subtree ! c
      case None =>
        val c = ContainsResult(id, result = false)
        log.debug("searchSubTree-{}, no subtree, returning {} to {}", p, c, requester)
        requester ! c
    }

  private def goToNode(opName: String,
                       requester: ActorRef,
                       subtrees: Map[Position, ActorRef],
                       p: Position,
                       id: Int)
                      (ifFound: => Operation)
                      (ifNotFound: => (Map[Position, ActorRef], Boolean)): Unit = {
    subtrees.get(p) match {
      case Some(subtree) =>
        log.debug("{}: {} subtree: send {} to {}", opName, p, ifFound, subtree)
        subtree ! ifFound
      case None =>
        val (newSubtrees, newRemoved) = ifNotFound
        log.debug("{}: no {} subtree, walk stopped at elem {}", opName, p, elem)
        setNormalBehavior(opName, newSubtrees, newRemoved)
        tellOperationFinished(opName, requester, id)
    }
  }

  private def tellOperationFinished(name: String, requester: ActorRef, id: Int): Unit = {
    requester ! OperationFinished(id)
    log.debug("{} sent OperationFinished({}) to {}", name, id, requester)
  }

  private def setNormalBehavior(opName: String, subtrees: Map[Position, ActorRef], removed: Boolean): Unit = {
    context.become(normal(subtrees, removed))
    log.debug("{}: context set to normal({}, {})", opName, subtrees, removed)
  }

  /** Handles `Operation` messages and `CopyTo` requests. */
  def normal(subtrees: Map[Position, ActorRef], removed: Boolean): Receive = LoggingReceive {
    case Contains(requester, id, searchingForElem) if searchingForElem == elem =>
      log.debug("Contains id{}, subtrees = {}, removed = {}", id, subtrees, removed)
      val c = ContainsResult(id, ! removed)
      log.debug("Contains found {}, removed = {}, sending {} to {}",
        elem, removed, c, requester)
      requester ! c
    case Contains(requester, id, searchingForElem) =>
      val direction = if (searchingForElem < elem) Left else Right
      log.debug("Contains id{}, subtrees = {}, removed = {}, search " + direction,
        id, subtrees, removed)
      searchSubTree(requester, subtrees, direction, id, searchingForElem)

    case Insert(requester, id, newElem) if newElem == elem =>
      val msg = if (removed) "but was removed, undeleting it" else "and not removed, no logical change"
      log.debug("Insert id{}, subtrees = {}, removed = {}", id, subtrees, removed)
      log.debug("id{} elem {} found " + msg, id, newElem)
      setNormalBehavior(Insert.Name, subtrees, removed = false)
      tellOperationFinished(Insert.Name, requester, id)
    case Insert(requester, id, newElem) =>
      val direction = if (newElem < elem) Left else Right
      def ifFound = Insert(requester, id, newElem)
      def ifNotFound = {
        val newNode = context.actorOf(Props(classOf[BinaryTreeNode], newElem, false),
          s"elem-$newElem")
        log.debug("no {} subtree, add new actor {}", direction,newNode)
        val newSubTrees = subtrees + (direction -> newNode)
        (newSubTrees, removed)
      }
      log.debug("Insert id{} elem {}, subtrees = {}, removed = {}, go " + direction,
        id, newElem, subtrees, removed)
      goToNode(Insert.Name, requester, subtrees, direction, id)(ifFound)(ifNotFound)

    case Remove(requester, id, target) if target == elem =>
      val msg = if (removed) "already removed so no logical change" else "change behavior to removed = true"
      log.debug("Remove id{} elem {}, subtrees = {}, removed = {}, " + msg,
        id, target, subtrees, removed)
      setNormalBehavior(Remove.Name, subtrees, removed = true)
      tellOperationFinished(Remove.Name, requester, id)
    case Remove(requester, id, target) =>
      val direction = if (target < elem) Left else Right
      def ifFound = Remove(requester, id, target)
      def ifNotFound = {
        log.debug("no {} subtree, element not found so just return", direction)
        (subtrees, removed)  // removing when not found means not changing our state, return current state
      }
      log.debug("Remove id{} elem {}, subtrees = {}, removed = {}, go " + direction,
        id, target, subtrees, removed)
      goToNode(Remove.Name, requester, subtrees, direction, id)(ifFound)(ifNotFound)

    case _ => ???
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = ???


}
