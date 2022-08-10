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

  /** Request to perform garbage collection */
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor with ActorLogging {
  import BinaryTreeSet._

  val defaultRootElement = 0

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

    case _ => ???
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

  private def insertSubTree(requester: ActorRef,
                            subtrees: Map[Position, ActorRef],
                            removed: Boolean,
                            p: Position, id: Int, newElem: Int): Unit = {
    subtrees.get(p) match {
      case Some(subtree) =>
        val i = Insert(requester, id, newElem)
        log.debug("Insert into {} subtree: send {} to {}", p,i, subtree)
        subtree ! i
      case None =>
        val newNode = context.actorOf(Props(classOf[BinaryTreeNode], newElem, false),
          s"elem-$newElem")
        log.debug("no {} subtree, add new actor {}", p,newNode)
        val newSubTrees = subtrees + (p -> newNode)
        context.become(normal(newSubTrees, removed))
        log.debug("context set to normal({}, {})", newSubTrees, removed)
        requester ! OperationFinished(id)
        log.debug("Insert sent OperationFinished({}) to {}", id, requester)
    }
  }

  /** Handles `Operation` messages and `CopyTo` requests. */
  def normal(subtrees: Map[Position, ActorRef], removed: Boolean): Receive = LoggingReceive {
    case Contains(requester, id, searchingForElem) if searchingForElem == elem =>
      log.debug("Contains id{}, subtrees = {}, removed = {}", id, subtrees, removed)
      val c = ContainsResult(id, ! removed)
      log.debug("Contains found {}, removed = {}, sending {} to {}",
        elem, removed, c, requester)
      requester ! c
    case Contains(requester, id, searchingForElem) if searchingForElem < elem =>
      log.debug("Contains id{} searchingFor < elem, subtrees = {}, removed = {}, search left",
        id, subtrees, removed)
      searchSubTree(requester, subtrees, Left, id, searchingForElem)
    case Contains(requester, id, searchingForElem) =>
      log.debug("Contains id{} searchingFor > elem, subtrees = {}, removed = {}, search right",
        id, subtrees, removed)
      searchSubTree(requester, subtrees, Right, id, searchingForElem)

    case Insert(requester, id, newElem) if newElem == elem && ! removed =>
      log.debug("Insert id{}, subtrees = {}, removed = {}", id, subtrees, removed)
      log.debug("id{} elem {} found, sent OperationFinished({}) to {}",
        id, newElem, id, requester)
      requester ! OperationFinished(id)
      log.debug("Insert sent OperationFinished({}) to {}", id, requester)
    case Insert(requester, id, newElem) if newElem == elem =>
      log.debug("Insert id{}, subtrees = {}, removed = {}", id, subtrees, removed)
      log.debug("id{} elem {} found but was removed, undeleting it", id, newElem)
      context.become(normal(subtrees, removed = false))
      log.debug("context set to normal({}, false)", subtrees)
      requester ! OperationFinished(id)
      log.debug("Insert sent OperationFinished({}) to {}", id, requester)
    case Insert(requester, id, newElem) if newElem < elem =>
      log.debug("Insert id{} elem {}, subtrees = {}, removed = {}, go left",
        id, newElem, subtrees, removed)
      insertSubTree(requester, subtrees, removed, Left, id, newElem)
    case Insert(requester, id, newElem) =>
      log.debug("Insert id{} elem {}, subtrees = {}, removed = {}, go right",
        id, newElem, subtrees, removed)
      insertSubTree(requester, subtrees, removed, Right, id, newElem)

    case _ => ???
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = ???


}
