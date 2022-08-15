/**
  * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
  *
  *   @note To see debug messages, add -Dakka.loglevel=DEBUG -Dakka.actor.debug.receive=on to VM Options.
  *         For some tests, you may need to direct the console logs to a file to see all the messages.
  */

/*
I've tried 2 new things with this assignment.  First, I learned that my old habit of using debug.log(s"...") meant
the string formatter always got invoked, even if log level debug is turned off.  I used the simple string with
{} syntax here.  The positive is that the substitutions only happen if logging is active, so in a production
environment this helps with performance.  Three negatives:
- I find the syntax harder to read since the variables are not inline with the rest of the message.
- The substitution syntax only supports 4 arguments.  More than that, you have to fall back to string construction.
- Worse, it is EASY to forget to include an argument or get them misaligned, which makes getting the code right the
first time harder.

Second, I tried a mix of passing state only in the Receive methods and using vars.  BinaryTreeNode has no vars for
state values.  This has the benefit of keeping state changes local and with the messages (which is really good).
It has the detriment that you are passing additional parameters around that are only necessary when state has to
flow between helper methods.

The vars approach has the advantage that you don't have the additional code cluttering up the logic.  But it means
the code has vars in it, and I've always fought to not have vars.  In any event, DO NOT USE MUTABLE DATA STRUCTURES
to hold state if you use vars.  It is fine within the actor itself, but if you need to provide that data structure
to another actor in a message, it is REALLY EASY to accidentally pass the mutable version (which violates the Actor
design rule by having multiple, uncoordinated actors accessing the same memory at the same time).  If you use a
MUTABLE data structure in an Actor, you must NEVER include it in a message to another actor.  Instead, you must send
an immutable copy of it.  Remembering to do that is the trick....

Overall, while I appreciate the easier to read code with vars, I found keeping state as parameters in the Receive
methods to be easier to get correct the first time.
 */

package actorbintree

import actorbintree.BinaryTreeNode.CopyTo
import akka.actor._
import akka.event.LoggingReceive

import scala.annotation.tailrec
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

  /** This is the element inserted into a new BinaryTreeSet when the BinaryTreeSet is created. */
  val defaultRootElement = 0

} // object BinaryTreeSet


class BinaryTreeSet extends Actor with ActorLogging {
  import BinaryTreeSet._

  def createRoot: ActorRef = context.actorOf(
    Props(classOf[BinaryTreeNode], defaultRootElement, true),
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

    case GC =>
      val newRoot = context.actorOf(
        Props(classOf[BinaryTreeNode], defaultRootElement, true),
        s"NewRoot-elem-$defaultRootElement")
      log.debug("tell {} to CopyTo({})", root, newRoot)
      root ! CopyTo(newRoot)
      context.become(garbageCollecting(newRoot))
      log.debug("changed state to garbageCollecting({})", newRoot)
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = LoggingReceive {
    case op: Operation =>
      pendingQueue = pendingQueue.enqueue(op)
      log.debug("GC BinaryTreeSet: received and queued {}, pending queue now {}",
        op, pendingQueue)

    case BinaryTreeNode.CopyFinished =>
      log.debug("GC BinaryTreeSet: GCCompleted received")
      root = newRoot
      log.debug("GC BinaryTreeSet: set root = {}", newRoot)
      playQueuedOperations(pendingQueue)
      pendingQueue = Queue.empty[Operation]
      log.debug("GC BinaryTreeSet: pending queue reset to {}", pendingQueue)
      context.become(receive)
      log.debug("GC BinaryTreeSet: set context back to normal, GC complete")
  }

  @tailrec
  private def playQueuedOperations(q: Queue[Operation]): Unit = q.dequeueOption match {
    case None =>
      // empty queue, we're done
      ()
    case Some((operation, qWithoutOperation)) =>
      log.debug("GC BinaryTreeSet: sending queued operation {} to {}", operation, root)
      root ! operation
      playQueuedOperations(qWithoutOperation)
  }

} // class BinaryTreeSet

object BinaryTreeNode {
  sealed trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  /**
    * Acknowledges that a copy has been completed. This message should be sent
    * from a node to its parent, when this node and all its children nodes have
    * finished being copied.
    */
  case object CopyFinished

} // object BinaryTreeNode

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

    case CopyTo(node) =>
      // Insert myself into the new BinaryTreeSet `node` if I'm not removed.  I need an ID.  Would be nice to be unique.
      // What's unique?  My element value!  So use that as my message ID for garbage collecting.
      if (! removed) {
        val id = elem
        log.debug("GC BinaryTreeNode: {} valid to copy, inserting elem {} id {} into {}",
          self, elem, id, node)
        node ! Insert(self, id, elem)
        log.debug("GC BinaryTreeNode: {} change state to awaitInsertCompleted({})", self, node)
        context.become(awaitInsertCompleted(node, subtrees))
      } else {
        log.debug("GC BinaryTreeNode: {} removed, DID NOT insert into new BinaryTreeSet", self)
        tellChildrenCopyTo(subtrees.values.toList, node)
      }

  }

  // Looking for an OperationFinished message with my `elem` as its message id.  Assuming I get it,
  // then tell my children to copy themselves.  That routine will take care of finishing the CopyTo operation.
  private def awaitInsertCompleted(node: ActorRef, subtrees: Map[Position, ActorRef]): Receive = LoggingReceive {
    case OperationFinished(id: Int) if id == elem =>
        log.debug("GC BinaryTreeNode awaitInsertCompleted: got expected Insert completed notification " +
          "id {} from {}", id, sender())
        tellChildrenCopyTo(subtrees.values.toList, node)
  }

  // Tell my children to CopyTo(node).  But if I have no children, I'm actually Done copying.
  private def tellChildrenCopyTo(children: List[ActorRef], node: ActorRef): Unit = {
    if (children.isEmpty) sendGCCompleted() else {
      log.debug("GC BinaryTreeNode: {} has children {}, tell them to CopyTo({})",
        self, children, node)
      children.foreach(_ ! CopyTo(node))
      log.debug("GC BinaryTreeNode: {} state becomes awaitGCCompleted({})", self, children)
      context.become(awaitGCCompleted(children))
    }
  }

  // Because each BinaryTreeNode stops itself once it is finished doing CopyTo(), this current instance cannot
  // return to its `normal` state.  That `normal` state now exists in the new BinaryTreeSet.  This means we do
  // not need to remember our old children (because they are stopping themselves when they are done CopyTo) or
  // whether or not we are removed.
  private def awaitGCCompleted(waitingForChildren: List[ActorRef]): Receive = LoggingReceive {
    case CopyFinished =>
      val nowWaitingFor = waitingForChildren.filter(_ != sender())
      if (nowWaitingFor.isEmpty) {
        log.debug("GC BinaryTreeNode awaitGCCompleted: {} children finished", self)
        sendGCCompleted()
      } else {
        log.debug("GC BinaryTreeNode awaitGCCompleted: {} still waiting for {}",
          self, nowWaitingFor)
        context.become(awaitGCCompleted(nowWaitingFor))
      }
  }

  private def sendGCCompleted(): Unit = {
    log.debug("GC BinaryTreeNode: finished in {}, send GCCompleted to {}", self, context.parent)
    context.parent ! CopyFinished
    log.debug("GC BinaryTreeNode: purposely stopping {}", self)
    context.stop(self)
  }

} // class BinaryTreeNode
