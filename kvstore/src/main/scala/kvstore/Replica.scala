// Originally I used the state stack with becomes(), and ran into intermittent trouble
// with where the popped state returned.  Rather than continue trying to solve that, I decided
// to start over with all the data management in a State container, so there is almost no context
// switching here.
//
// One of my goals was to eliminate using the var keyword.  I found this problematic, because all the auxiliary
// routines would have to receive the state as a parameter, return the modified state, and the caller then use
// context.becomes() to make the state changes available to the next message.  Doable, but really clutters the code.
// And this class is already hard enough to read as it is.  Trying to decompose the variable pieces of state into
// their own modules gets us right back to passing state around in a parameter again.  So I caved, and went with
// a single var, state of type State, to at least gather the Actor state in one spot.
//
// It also bothered me greatly at how large the Receive handlers became for this assignment.  As
// I was trying to think of a better way, I remembered that Receive handlers are partial functions,
// and that one way to implement Chain of Responsibility in Scala is with a sequence of partial
// functions.  The various links in the chain are connected with orElse.  This lets me define
// separate Receive handlers for larger-grained operations, and mix and match them to implement
// the leader and secondary handlers.  I rather like how this separates the cases into highly-related
// items, and lets one focus on just those cases instead of these gigantic Receive partial functions.
// The only side effect is that you get excess "unhandled message" debug log entries for messages
// that aren't covered by the first group of partial functions.  The important thing is to see that
// eventually the message with the particular id/seq number is handled, and not let those log entries
// bother you.
//
// Note about scheduled messages:  I'm not cancelling scheduled messages (e.g. SendPendingPersists, TimedOut)
// if they are no longer needed.  That leads to dead-letter logs from those messages sometimes, because I didn't
// go to the extra work to cancel them when the Actor got stopped.  It doesn't affect the assignment, just
// distracting in the logs when debug stuff is turned on.

package kvstore

import akka.actor.{Actor, ActorLogging, ActorRef, OneForOneStrategy, Props, SupervisorStrategy, Terminated}
import akka.event.LoggingReceive
import kvstore.Arbiter._

import scala.concurrent.duration._

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor with ActorLogging {
  import Replica._
  import Replicator._
  import akka.actor.SupervisorStrategy._
  import Persistence._
  import context.dispatcher

  override val supervisorStrategy: SupervisorStrategy = OneForOneStrategy(withinTimeRange = 1.second) {
    case _: Exception =>
      Restart
  }

  /** Holds the state information for this Actor.  Unlike Replicator, Replica has a complex state and it is quite
    * cumbersome to pass the state around to all message handlers and all support routines, and then combine
    * state changes from those routines back together.  Breaking down and using 1 var is just more pragmatic in this
    * case.
    *
    * @param persistence  the Actor providing the pretend persistence service
    * @param kv           the key-value store
    * @param secondaries  maps between a non-primary replica and its replicator.  The leader does not have a replicator.
    * @param pendingOperations  maps between IDs and operations that haven't completed
    * @param pendingReplicates  maps between IDs and Replicate operations that haven't completed
    * @param pendingPersists  maps between sequence numbers and Persist operations that haven't completed. IDs and
    *                         sequence numbers are not the same thing.
    */
  private case class State(persistence: ActorRef,
                           kv: Map[String, String],
                           secondaries: Map[ActorRef, ActorRef],
                           pendingOperations: Map[Long, PendingOperation],
                           pendingReplicates: Map[Long, PendingReplicate],
                           pendingPersists: Map[Long, PendingPersist])
  //noinspection ActorMutableStateInspection
  private var state = State(createPersistence, kv = Map.empty, secondaries = Map.empty,
    pendingOperations = Map.empty, pendingReplicates = Map.empty, pendingPersists = Map.empty)

  private def createPersistence = {
    val persistence = context.actorOf(persistenceProps)
    context.watch(persistence)
    persistence
  }

  private case class TimedOut(id: Long)
  private def enableOperationTimeout(id: Long): Unit = {
    context.system.scheduler.scheduleAtFixedRate(1.second, 1.second,
      self, TimedOut(id))
  }

  private def TimeOuts: Receive = LoggingReceive {
    case TimedOut(id: Long) =>
      removePendingOperation(id)(_.messageIfFailed)
      removePendingPersist(id)
      state.secondaries.get(self).foreach( replicator => removePendingReplicate(id, replicator) )
  }

  private case class PendingOperation(notifyWhenDone: ActorRef,
                                      messageIfSucceeded: Option[Any],
                                      messageIfFailed: Option[Any])
  private def addPendingOperation(id: Long, notifyWhenDone: ActorRef,
                                  messageIfSucceeded: Option[Any],
                                  messageIfFailed: Option[Any]): Unit = {
    state = state.copy(pendingOperations = state.pendingOperations +
      (id -> PendingOperation(notifyWhenDone, messageIfSucceeded, messageIfFailed)))
  }
  private def removePendingOperation(id: Long)(getMessage: PendingOperation => Option[Any]): Unit = {
    for (pendingOperation <- state.pendingOperations.get(id);
         message <- getMessage(pendingOperation)) {
      pendingOperation.notifyWhenDone ! message
    }
    state = state.copy(pendingOperations = state.pendingOperations - id)
  }

  private case class PendingReplicate(message: Any, stillWaitingOn: Set[ActorRef])
  private def addPendingReplicate(id: Long, replicate: Replicate): Unit = {
    // Only do this if there are any secondaries, because Leader doesn't have a Replicator.
    if (state.secondaries.nonEmpty) {
      val replicators = state.secondaries.values.toSet
      state = state.copy(pendingReplicates =
        state.pendingReplicates + (id -> PendingReplicate(replicate, replicators)))
      replicators.foreach(_ ! replicate)
    }
  }
  private def removePendingReplicate(id: Long, finished: ActorRef): Unit = {
    state.pendingReplicates
      .get(id)
      // remove `finished`, if stillWaitingOn becomes empty set, all pending replicates are done
      .map { pr: PendingReplicate => pr.copy(stillWaitingOn = pr.stillWaitingOn - finished) }
      .foreach {
        case pr: PendingReplicate if pr.stillWaitingOn.isEmpty =>
          // all the replicators have finished so we are done replicating
          state = state.copy(pendingReplicates = state.pendingReplicates.removed(id))
        case replicationNotAllFinished: PendingReplicate =>
          // we are still awaiting some replication acknowledgements, so update the state for this id.
          state = state.copy(pendingReplicates = state.pendingReplicates.updated(id, replicationNotAllFinished))
      }
  }

  private case class PendingPersist(notifyWhenDone: ActorRef, message: Persist)
  private case object SendPendingPersists
  private def addPendingPersist(notifyWhenDone: ActorRef, seq: Long, persist: Persist): Unit = {
    state = state.copy(pendingPersists =
      state.pendingPersists + (seq -> PendingPersist(notifyWhenDone, persist)))
    state.persistence ! persist
  }
  private def removePendingPersist(seq: Long): Unit = { state = state.copy(
    pendingPersists = state.pendingPersists - seq)
  }

  private def tryToFinishOperation(id: Long): Unit = {
    (state.pendingReplicates.get(id), state.pendingPersists.get(id)) match {
      case (None, None) =>
        removePendingOperation(id)(_.messageIfSucceeded)
      case _ =>
      // not totally finished, so don't do anything
    }
  }

  def receive: Receive = LoggingReceive {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  private val Gets: Receive = LoggingReceive {
    case Get(key: String, id: Long) =>
      sender() ! GetResult(key, state.kv.get(key), id)
  }

  private val Updates: Receive = LoggingReceive {
    case Insert(key: String, value: String, id: Long) =>
      state = state.copy(kv = state.kv + (key->value))
      addPendingOperation(id, sender(),
        Some(OperationAck(id)), Some(OperationFailed(id)))
      addPendingReplicate(id, Replicate(key, Some(value), id))
      addPendingPersist(self, id, Persist(key, Some(value), id))
      enableOperationTimeout(id)
    case Remove(key: String, id: Long) =>
      state = state.copy(kv = state.kv - key)
      addPendingOperation(id, sender(),
        Some(OperationAck(id)), Some(OperationFailed(id)))
      addPendingReplicate(id, Replicate(key, None, id))
      addPendingPersist(self, id, Persist(key, None, id))
      enableOperationTimeout(id)
  }

  private val expectedSeqId = new LongGenerator()

  private val Snapshots: Receive = LoggingReceive {
    case Snapshot(key: String, _: Option[String], seq: Long) if seq < expectedSeqId.value =>
      // Already seen it, immediately acknowledge
      sender() ! SnapshotAck(key, seq)
    case Snapshot(_: String, _: Option[String], seq: Long) if seq > expectedSeqId.value =>
      // Ignore it, force Replicator to send it again once we've caught up.
      ()
    case Snapshot(key: String, value: Option[String], seq: Long) =>
      value match {
        case Some(v) => state = state.copy(kv = state.kv + (key->v))
        case None =>    state = state.copy(kv = state.kv - key)
      }
      addPendingOperation(seq, sender(),
        Some(SnapshotAck(key, seq)), None)
      addPendingPersist(sender(), seq, Persist(key, value, seq))
      enableOperationTimeout(seq)
      expectedSeqId.next()
  }

  private val Persists: Receive = LoggingReceive {
    case Persisted(_, seq) =>
      removePendingPersist(seq)
      tryToFinishOperation(seq)
    case SendPendingPersists =>
      state.pendingPersists.foreach { case (_, pendingPersist) => state.persistence ! pendingPersist.message }
    case Terminated(actor) if actor == state.persistence =>
      state = state.copy(persistence = createPersistence)
      self ! SendPendingPersists
  }

  private val Replicates: Receive = LoggingReceive {
    case Replicated(_, id) =>
      removePendingReplicate(id, sender())
      tryToFinishOperation(id)
  }

  private def addSecondary(secondary: ActorRef): Unit = {
    val replicator = context.system.actorOf(Replicator.props(secondary))
    state = state.copy(secondaries = state.secondaries + (secondary -> replicator))
    // Need to "make up" id numbers to use with these new Replicate messages.  Values don't matter,
    // so zipWithIndex is perfect here.
    state.kv.zipWithIndex.foreach {
      case ((key, value), id) =>
        replicator ! Replicate(key, Some(value), id)
    }
  }
  private def removeSecondary(secondary: ActorRef): Unit = {
    state.secondaries.get(secondary).foreach { replicator =>
      state.pendingPersists
        .filter { case (_, pendingPersist) => pendingPersist.notifyWhenDone == secondary}
        .foreach { case (seq, pendingPersist) =>
          removePendingPersist(seq)
          tryToFinishOperation(pendingPersist.message.id)
        }
      state.pendingReplicates
        .filter { case (_, pendingReplicate) => pendingReplicate.stillWaitingOn.contains(replicator) }
        .foreach { case (id, _) =>
          removePendingReplicate(id, replicator)
          tryToFinishOperation(id)
        }
      context.stop(secondary)  // The replicator will see secondary stopped and stop itself via DeathWatch.
      state = state.copy(secondaries = state.secondaries - secondary)
    }
  }

  private def UpdateReplicas: Receive = LoggingReceive {
    case Replicas(replicas) =>
      val possibleSecondaries = replicas - self
      val secondariesKeySet = state.secondaries.keySet
      val toBeRemoved = secondariesKeySet -- possibleSecondaries
      toBeRemoved.foreach(removeSecondary)
      val newSecondaries = possibleSecondaries -- secondariesKeySet
      newSecondaries.foreach(addSecondary)
  }

  // Here is where Chain of Responsibility comes in handy!
  private val leader: Receive = Gets orElse Updates orElse Persists orElse Replicates orElse TimeOuts orElse UpdateReplicas
  private val replica: Receive = Gets orElse Snapshots orElse Persists orElse TimeOuts

  // Resend any pending persists every 100 milliseconds, per homework.
  context.system.scheduler.scheduleAtFixedRate(100.milliseconds, 100.milliseconds,
    self, SendPendingPersists)

  // leave this at the absolute bottom so it happens last
  arbiter ! Join
}

