package kvstore

import akka.actor.{Actor, ActorLogging, ActorRef, OneForOneStrategy, Props, ReceiveTimeout, SupervisorStrategy}
import akka.event.LoggingReceive
import kvstore.Arbiter._
import kvstore.Persistence.{Persist, Persisted, PersistenceException}

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

  private val PersistenceTimeout = 100.milliseconds
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
  var secondaries = Map.empty[ActorRef, Replicator]
  // the current set of replicators
  var replicators = Set.empty[Replicator]


  def receive: Receive = LoggingReceive {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  // We never add or remove a leader in this assignment.  That means the caller needs to remove `self` from
  // the replicas passed here.
  private def handleAddedReplicas(replicasWithoutMe: Set[ActorRef]): Unit = {
    // need to know what replicas in newSecondaries haven't already been seen.  Could be empty.
    val newlyAddedReplicas: Set[ActorRef] = replicasWithoutMe -- secondaries.keySet
    // make Replicators for each new replica, send the Replicate message for the contents of the store,
    // and return the new replica -> new replicator pair to then update our state.
    val newSecondaryMapEntries: Set[(ActorRef, Replicator)] = for (toBeAdded <- newlyAddedReplicas) yield {
      val replicator = new Replicator(toBeAdded)
      val ids= LazyList.from(1).iterator  // Ints for ids here are good enough for homework.
      kv.foreach { case (key, value) => replicator.self ! Replicate(key, Some(value), ids.next()) }
      toBeAdded -> replicator
    }
    secondaries ++= newSecondaryMapEntries
    replicators ++= newSecondaryMapEntries.map(_._2)
  }

  private def handleRemovedReplicas(replicasWithoutMe: Set[ActorRef]): Unit = {
    // also need to know what replicas need to have Replicators stopped.  Could be empty.
    val newlyRemovedReplicas: Set[ActorRef] = secondaries.keySet -- replicasWithoutMe
    // stop every Replicator associated with a removed replica, and return those replicators to update our state.
    val newlyRemovedReplicators: Set[Replicator] = for (toBeStopped <- newlyRemovedReplicas;
                                                        replicator = secondaries(toBeStopped)
                                                        ) yield {
      context.stop(replicator.self)
      replicator
    }
    secondaries --= newlyRemovedReplicas
    replicators --= newlyRemovedReplicators
  }

  /* TODO Behavior for  the leader role. */
  // TODO failure handling and sending of OperationFailed(id)
  val leader: Receive = LoggingReceive {
    case Insert(key: String, value: String, id: Long) =>
      kv += key -> value
      replicators.map(_.self).foreach(_ ! Replicate(key, Some(value), id))
      sender() ! OperationAck(id)
    case Remove(key: String, id: Long) =>
      kv -= key
      replicators.map(_.self).foreach(_ ! Replicate(key, None, id))
      sender() ! OperationAck(id)
    case Get(key: String, id: Long) =>
      sender() ! GetResult(key, kv.get(key), id)

    case Replicas(replicas: Set[ActorRef]) =>
      val replicasWithoutMe = replicas - self
      handleAddedReplicas(replicasWithoutMe)
      handleRemovedReplicas(replicasWithoutMe)

    case Replicated(key: String, id: Long) =>
    // TODO unsure yet what to do with this message when I receive it.  Just know that leader will receive it.

  }  // leader Receive handler.

  var expectedSnapshotSequenceNumber: Long = 0L

  /* TODO Behavior for the replica role. */
  val replica: Receive = LoggingReceive {
    case Get(key: String, id: Long) =>
      sender() ! GetResult(key, kv.get(key), id)

    case Snapshot(_, _, seq: Long) if seq > expectedSnapshotSequenceNumber =>
      // snapshot must be ignored (no state change and no reaction).  Sender will have to resend it.
      ()
    case Snapshot(key: String, _, seq: Long) if seq < expectedSnapshotSequenceNumber =>
      // already seen a later update, so just ack it
      sender() ! SnapshotAck(key, seq)
    case Snapshot(key: String, value: Option[String], seq: Long) if seq == expectedSnapshotSequenceNumber =>
      value match {
        case Some(aValue) =>
          kv += key -> aValue
        case None =>
          kv -= key
      }
      val persistMessage = Persist(key, value, seq)
      persistence ! persistMessage
      context.setReceiveTimeout(PersistenceTimeout)
      context.become(secondaryAwaitPersisted(sender(), persistMessage))

  }  // secondary Receive handler

  def secondaryAwaitPersisted(snapshotRequester: ActorRef, persistMessage: Persist): Receive = LoggingReceive {
    case Get(key: String, id: Long) =>
      sender() ! GetResult(key, kv.get(key), id)

    // Thoughts on snapshots while waiting for Persisted message.  If we are here, then we have not
    // yet updated the expectedSnapshotSequenceNumber.  Which means if we receive a Snapshot message with an
    // older ID, we need to immediately acknowledge it and do nothing else (like normal).  If we receive a NEW
    // Snapshot message, it will have a sequence number higher than what we are currently working on.  We can ignore
    // it, the requester will eventually resend it hopefully after we've finished here.  If we get the same sequence
    // number we are already working on, we just ignore the message because we're still working on it.  So... I
    // don't have to queue up Snapshot messages and replay them.  I only have to acknowledge messages with older
    // sequence numbers.
    case Snapshot(key: String, _, seq: Long) if seq < expectedSnapshotSequenceNumber =>
      sender() ! SnapshotAck(key, seq)
    case _: Snapshot =>
      ()

    case Persisted(key: String, seq: Long) if seq == expectedSnapshotSequenceNumber =>
      // seq+1 is really good enough--we already know seq == expectedSnapshotSequenceNumber--but this line
      // matches the homework specification.
      expectedSnapshotSequenceNumber = Math.max(expectedSnapshotSequenceNumber, seq + 1)
      snapshotRequester ! SnapshotAck(key, seq)
      context.setReceiveTimeout(Duration.Undefined)
      context.become(replica)

    case ReceiveTimeout =>
      persistence ! persistMessage
  }

  // Leave this at the bottom to make sure it always happens last.
  arbiter ! Join

} // class Replica

