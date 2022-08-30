package kvstore

import akka.actor.{Actor, ActorLogging, ActorRef, OneForOneStrategy, Props, ReceiveTimeout, SupervisorStrategy}
import akka.event.LoggingReceive
import kvstore.Arbiter._
import kvstore.Persistence.{Persist, Persisted, PersistenceException}

import scala.collection.immutable.Queue
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

  case class PersistFailed(id: Long)

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))

  private val PersistenceTimeout = 95.milliseconds
  private val MaxTimeUntilPersistFailed: FiniteDuration = 1000.milliseconds
} // object Replica

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor with ActorLogging {
  import Replica._
  import Replicator._

  private val persistence = context.actorOf(persistenceProps)

  override val supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case _: PersistenceException => SupervisorStrategy.Restart
  }

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]


  def receive: Receive = LoggingReceive {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  // We never add or remove a leader in this assignment.  That means the caller needs to remove `self` from
  // the replicas passed here.
  private def handleAddedReplicas(newReplicaSet: Set[ActorRef]): Unit = {
    // need to know what replicas in newSecondaries haven't already been seen.  Could be empty.
    val newlyAddedReplicas: Set[ActorRef] = newReplicaSet -- secondaries.keySet
    // make Replicators for each new replica, send the Replicate message for the contents of the store,
    // and return the new replica -> new replicator pair to then update our state.
    val newSecondaryMapEntries: Set[(ActorRef, ActorRef)] = for (toBeAdded <- newlyAddedReplicas) yield {
      val replicator = context.actorOf(Props(new Replicator(toBeAdded)))
      val ids= LazyList.from(1).iterator  // Ints for ids here are good enough for homework.
      kv.foreach { case (key, value) => replicator ! Replicate(key, Some(value), ids.next()) }
      toBeAdded -> replicator
    }
    secondaries ++= newSecondaryMapEntries
    replicators ++= newSecondaryMapEntries.map(_._2)
  }

  private def handleRemovedReplicas(newReplicaSet: Set[ActorRef]): Unit = {
    // also need to know what replicas need to have Replicators stopped.  Could be empty.
    val newlyRemovedReplicas: Set[ActorRef] = secondaries.keySet -- newReplicaSet
    // stop every Replicator associated with a removed replica, and return those replicators to update our state.
    val newlyRemovedReplicators: Set[ActorRef] = for (toBeStopped <- newlyRemovedReplicas;
                                                      replicator = secondaries(toBeStopped)
                                                      ) yield {
      context.stop(replicator)
      replicator
    }
    secondaries --= newlyRemovedReplicas
    replicators --= newlyRemovedReplicators
  }

  /* TODO Behavior for  the leader role. */
  // TODO failure handling and sending of OperationFailed(id)
  val leader: Receive = LoggingReceive {
    case msg @ Insert(key: String, value: String, id: Long) =>
      log.debug("leader: Insert: {}", msg)
      log.debug("leader: Insert: replicators = {}", replicators)
      kv += key -> value
      replicators.foreach(_ ! Replicate(key, Some(value), id))
      log.debug("leader: Insert: pushed awaitReplicated({})", sender())
      context.become(awaitReplicated(sender()), discardOld = false)

    case Remove(key: String, id: Long) =>
      kv -= key
      replicators.foreach(_ ! Replicate(key, None, id))
      log.debug("leader: Remove: pushed awaitReplicated({})", sender())
      context.become(awaitReplicated(sender()), discardOld = false)

    case Get(key: String, id: Long) =>
      sender() ! GetResult(key, kv.get(key), id)

    case Replicas(replicas: Set[ActorRef]) =>
      handleAddedReplicas(replicas)
      handleRemovedReplicas(replicas)
      log.debug("leader: Replicas: after Replicas message, secondaries = {}, replicators = {}", secondaries, replicators)

  }  // leader Receive handler.

  var pendingQueue: Queue[Operation] = Queue.empty[Operation]

  private def awaitReplicated(client: ActorRef): Receive = LoggingReceive {
    case msg @ Get(key: String, id: Long) =>
      log.debug("awaitReplicated: Get: msg = {}", msg)
      sender() ! GetResult(key, kv.get(key), id)

    case op: Operation =>
      log.debug("awaitReplicated: Operation: msg = {}", op)
      pendingQueue.enqueue(op)

    // Here we CAN receive Snapshot messages just like a secondary and need to react to them the same.
    case msg @ Snapshot(_, _, seq: Long) if seq > expectedSnapshotSequenceNumber =>
      // snapshot must be ignored (no state change and no reaction).  Sender will have to resend it.
      log.debug("awaitReplicated: Snapshot: msg = {}, ignored", msg)
      ()
    case msg @ Snapshot(key: String, _, seq: Long) if seq < expectedSnapshotSequenceNumber =>
      log.debug("awaitReplicated: Snapshot: msg = {}, immediate ack", msg)
      // already seen a later update, so just ack it
      sender() ! SnapshotAck(key, seq)
    case msg @ Snapshot(key: String, value: Option[String], seq: Long) if seq == expectedSnapshotSequenceNumber =>
      log.debug("awaitReplicated: Snapshot: msg = {}, updating", msg)
      value match {
        case Some(aValue) =>
          kv += key -> aValue
        case None =>
          kv -= key
      }
      val persistMessage = Persist(key, value, seq)
      persistence ! persistMessage
      context.setReceiveTimeout(PersistenceTimeout)
      log.debug("awaitReplicated: Snapshot: pushed secondaryAwaitPersisted({}, {})",sender(), persistMessage)
      context.become(secondaryAwaitPersisted(sender(), persistMessage,
        MaxTimeUntilPersistFailed), discardOld = false)

    case msg @ Replicated(_, id: Long) =>
      log.debug("awaitReplicated: Replicated: msg = {}, send OperationAck", msg)
      client ! OperationAck(id)
      pendingQueue.foreach(operation => self ! operation)
      pendingQueue = Queue.empty[Operation]
      log.debug("awaitReplicated: Replicated: become leader by popping")
      context.unbecome()

    case msg @ Replica.PersistFailed(id: Long) =>
      log.debug("awaitReplicated: PersistFailed: msg = {}", msg)
      client ! OperationFailed(id)
      log.debug("awaitReplicated: PersistFailed: popping context")
      context.unbecome()

  } // awaitReplicated()

  var expectedSnapshotSequenceNumber: Long = 0L

  /* TODO Behavior for the replica role. */
  val replica: Receive = LoggingReceive {
    case msg @ Get(key: String, id: Long) =>
      log.debug("replica: Get: msg = {}", msg)
      sender() ! GetResult(key, kv.get(key), id)

    case msg @ Snapshot(_, _, seq: Long) if seq > expectedSnapshotSequenceNumber =>
      log.debug("replica: Snapshot: ignored msg = {}", msg)
      // snapshot must be ignored (no state change and no reaction).  Sender will have to resend it.
      ()
    case msg @Snapshot(key: String, _, seq: Long) if seq < expectedSnapshotSequenceNumber =>
      log.debug("replica: Snapshot: immediately ack already processed msg = {}", msg)
      // already seen a later update, so just ack it
      sender() ! SnapshotAck(key, seq)
    case msg @ Snapshot(key: String, value: Option[String], seq: Long) if seq == expectedSnapshotSequenceNumber =>
      log.debug("replica: Snapshot: process msg = {}", msg)
      value match {
        case Some(aValue) =>
          kv += key -> aValue
        case None =>
          kv -= key
      }
      val persistMessage = Persist(key, value, seq)
      persistence ! persistMessage
      context.setReceiveTimeout(PersistenceTimeout)
      log.debug("replica: Snapshot: pushed secondaryAwaitPersisted({}, {}, {})",
        sender(), persistMessage, MaxTimeUntilPersistFailed)
      context.become(secondaryAwaitPersisted(sender(),
        persistMessage, MaxTimeUntilPersistFailed), discardOld = false)

    case msg @ Replica.PersistFailed(id: Long) =>
      log.debug("replica: PersistFailed: msg = {} forwarding to {}", msg, sender())
      sender() ! OperationFailed(id)

  }  // secondary Receive handler

  def secondaryAwaitPersisted(snapshotRequester: ActorRef,
                              persistMessage: Persist,
                              persistTimeRemaining: FiniteDuration): Receive = LoggingReceive {
    case msg @ Get(key: String, id: Long) =>
      log.debug("secondaryAwaitPersisted: Get: msg = {}", msg)
      sender() ! GetResult(key, kv.get(key), id)

    // Thoughts on snapshots while waiting for Persisted message.  If we are here, then we have not
    // yet updated the expectedSnapshotSequenceNumber.  Which means if we receive a Snapshot message with an
    // older ID, we need to immediately acknowledge it and do nothing else (like normal).  If we receive a NEW
    // Snapshot message, it will have a sequence number higher than what we are currently working on.  We can ignore
    // it, the requester will eventually resend it hopefully after we've finished here.  If we get the same sequence
    // number we are already working on, we just ignore the message because we're still working on it.  So... I
    // don't have to queue up Snapshot messages and replay them.  I only have to acknowledge messages with older
    // sequence numbers.
    case msg @ Snapshot(key: String, _, seq: Long) if seq < expectedSnapshotSequenceNumber =>
      log.debug("secondaryAwaitPersisted: Snapshot: msg = {}, ack immediately", msg)
      sender() ! SnapshotAck(key, seq)
    case msg: Snapshot =>
      log.debug("secondaryAwaitPersisted: Snapshot: msg = {}, ignored", msg)
      ()

    case msg @ Persisted(key: String, seq: Long) if seq == expectedSnapshotSequenceNumber =>
      log.debug("secondaryAwaitPersisted: Persisted: msg = {} with expected number, send SnapshotAck", msg)
      // seq+1 is really good enough--we already know seq == expectedSnapshotSequenceNumber--but this line
      // matches the homework specification.
      expectedSnapshotSequenceNumber = Math.max(expectedSnapshotSequenceNumber, seq + 1)
      snapshotRequester ! SnapshotAck(key, seq)
      context.setReceiveTimeout(Duration.Undefined)
      context.unbecome()
      log.debug("secondaryAwaitPersisted: Persisted: popped context stack")

    case ReceiveTimeout if persistTimeRemaining > 0.milliseconds =>
      log.debug("secondaryAwaitPersisted: ReceiveTimeout with {} remaining: resend {}",
        persistTimeRemaining, persistMessage)
      persistence ! persistMessage
      context.become(secondaryAwaitPersisted(snapshotRequester, persistMessage,
        persistTimeRemaining - PersistenceTimeout))  // in this case, DO NOT PUSH context.

    case ReceiveTimeout if persistTimeRemaining <= 0.milliseconds =>
      log.debug("secondaryAwaitPersisted: ReceiveTimeout no time left")
      snapshotRequester ! Replicator.PersistFailed(persistMessage)
      context.unbecome()

    case msg @ Replica.PersistFailed(id: Long) =>
      self ! Replica.PersistFailed(id)
      context.unbecome()
  }

  // Leave this at the bottom to make sure it always happens last.
  arbiter ! Join

} // class Replica

