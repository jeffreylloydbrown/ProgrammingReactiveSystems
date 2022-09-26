// Originally I used the state stack with becomes(), and ran into intermittent trouble
// with where the popped state returned.  Rather than continue trying to solve that, I decided
// to start over with all the data management in member variables, so there is almost no context
// switching here.
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

package kvstore

import akka.actor.{Actor, ActorRef, OneForOneStrategy, Props, SupervisorStrategy, Terminated}
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

//noinspection ActorMutableStateInspection
class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import akka.actor.SupervisorStrategy._
  import Persistence._
  import context.dispatcher

  override val supervisorStrategy: SupervisorStrategy = OneForOneStrategy(withinTimeRange = 1.second) {
    case _: Exception =>
      Restart
  }

  private def createPersistence = {
    val persistence = context.actorOf(persistenceProps)
    context.watch(persistence)
    persistence
  }

  private var persistence = createPersistence

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  private var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  private var secondaries = Map.empty[ActorRef, ActorRef]

  private case class PendingOperation(notifyWhenDone: ActorRef,
                                      messageIfSucceeded: Option[Any],
                                      messageIfFailed: Option[Any])
  private var pendingOperations = Map.empty[Long, PendingOperation]
  private def addPendingOperation(id: Long, notifyWhenDone: ActorRef,
                                  messageIfSucceeded: Option[Any],
                                  messageIfFailed: Option[Any]): Unit = {
    pendingOperations +=   id -> PendingOperation(notifyWhenDone, messageIfSucceeded, messageIfFailed)
  }
  private def removePendingOperation(id: Long)(getMessage: PendingOperation => Option[Any]): Unit = {
    for (pendingOperation <- pendingOperations.get(id);
         message <- getMessage(pendingOperation)) {
      pendingOperation.notifyWhenDone ! message
    }
    pendingOperations -= id
  }

  private case class PendingReplicate(message: Any, stillWaitingOn: Set[ActorRef]) {
    def anActorFinished(actor: ActorRef): PendingReplicate = this.copy(stillWaitingOn = stillWaitingOn - actor)
  }
  private var pendingReplicates = Map.empty[Long, PendingReplicate]
  private def addPendingReplicate(id: Long, replicate: Replicate): Unit = {
    // Only do this if there are any secondaries, because Leader doesn't have a Replicator.
    if (secondaries.nonEmpty) {
      val replicators = secondaries.values.toSet
      pendingReplicates +=    id -> PendingReplicate(replicate, replicators)
      replicators.foreach(_ ! replicate)
    }
  }
  private def removePendingReplicate(id: Long, finished: ActorRef): Unit = {
    pendingReplicates
      .get(id)
      .map { _.anActorFinished(finished) }  // stillWaitingOn becomes Set.empty if all done.
      .foreach {
        case pr: PendingReplicate if pr.stillWaitingOn.isEmpty =>
          // all the replicators have finished so we are done replicating
          pendingReplicates -= id
        case replicationNotAllFinished: PendingReplicate =>
          // we are still awaiting some replication acknowledgements, so update the state for this id.
          pendingReplicates.updated(id, replicationNotAllFinished)
      }
  }

  private case class PendingPersist(notifyWhenDone: ActorRef, message: Persist)
  private case object SendPendingPersists
  private var pendingPersists = Map.empty[Long, PendingPersist]
  private def addPendingPersist(notifyWhenDone: ActorRef, seq: Long, persist: Persist): Unit = {
    pendingPersists +=    seq -> PendingPersist(notifyWhenDone, persist)
    persistence ! persist
    context.system.scheduler.scheduleAtFixedRate(0.milliseconds, 100.milliseconds,
      self, SendPendingPersists)
  }
  private def removePendingPersist(seq: Long): Unit = { pendingPersists -= seq }

  private def tryToFinishOperation(id: Long): Unit = {
    (pendingReplicates.get(id), pendingPersists.get(id)) match {
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
      sender() ! GetResult(key, kv.get(key), id)
  }

  private val Updates: Receive = LoggingReceive {
    case Insert(key: String, value: String, id: Long) =>
      kv += key->value
      addPendingOperation(id, sender(),
        Some(OperationAck(id)), Some(OperationFailed(id)))
      addPendingReplicate(id, Replicate(key, Some(value), id))
      addPendingPersist(self, id, Persist(key, Some(value), id))
    case Remove(key: String, id: Long) =>
      kv -= key
      addPendingOperation(id, sender(),
        Some(OperationAck(id)), Some(OperationFailed(id)))
      addPendingReplicate(id, Replicate(key, None, id))
      addPendingPersist(self, id, Persist(key, None, id))
  }

  private var expectedSeqId = 0L

  private val Snapshots: Receive = LoggingReceive {
    case Snapshot(key: String, _: Option[String], seq: Long) if seq < expectedSeqId =>
      // Already seen it, immediately acknowledge
      sender() ! SnapshotAck(key, seq)
    case Snapshot(_: String, _: Option[String], seq: Long) if seq > expectedSeqId =>
      // Ignore it, force Replicator to send it again once we've caught up.
      ()
    case Snapshot(key: String, value: Option[String], seq: Long) =>
      value match {
        case Some(v) => kv += key->v
        case None => kv -= key
      }
      addPendingOperation(seq, sender(),
        Some(SnapshotAck(key, seq)), None)
      addPendingPersist(sender(), seq, Persist(key, value, seq))
      expectedSeqId = seq + 1
  }

  private val Persists: Receive = LoggingReceive {
    case Persisted(_, seq) =>
      removePendingPersist(seq)
      tryToFinishOperation(seq)
    case SendPendingPersists =>
      pendingPersists.foreach { case (_, pendingPersist) => persistence ! pendingPersist.message }
    case Terminated(actor) if actor == persistence =>
      persistence = createPersistence
      self ! SendPendingPersists
  }

  // Here is where Chain of Responsibility comes in handy!
  private val leader: Receive = Gets orElse Updates orElse Persists
  private val replica: Receive = Gets orElse Snapshots orElse Persists

  // leave this at the absolute bottom so it happens last
  arbiter ! Join
}

