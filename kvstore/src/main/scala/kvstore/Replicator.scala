package kvstore

import akka.actor.{Actor, ActorLogging, ActorRef, Props, ReceiveTimeout}
import akka.event.LoggingReceive
import kvstore.Persistence.Persist

import scala.concurrent.duration._

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)

  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  case class PersistFailed(persistMessage: Persist)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))

  private val SnapshotTimeout = 100.milliseconds
} // object Replicator

class Replicator(val replica: ActorRef) extends Actor with ActorLogging {
  import Replicator._

  // map from sequence number to pair of sender and request
  var awaitingSnapshotAcks = Map.empty[Long, (ActorRef, Replicate)]

  var _seqCounter = 0L
  private def nextSeq() = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  // These routines encapsulate setting and resetting our timeout.  Setting is straight-forward.  When we reset,
  // what we do depends on us waiting on SnapshotAcks to come back.  If there are none, then we don't need the
  // timeout anymore and we turn it off.  If there are SnapshotAcks still left to come, we keep the timeout in
  // place.  Since the timeout continues until deactivated, we do nothing if there are still SnapshotAcks coming.
  private def setTimeout(): Unit = context.setReceiveTimeout(SnapshotTimeout)
  private def resetTimeout(): Unit =
    if (awaitingSnapshotAcks.isEmpty) context.setReceiveTimeout(Duration.Undefined)


  def receive: Receive = LoggingReceive {
    case request @ Replicate(key: String, value: Option[String], _: Long) =>
      log.debug("Replicator receive: Replicate: request = {}", request)
      val mySeqNumber = nextSeq()
      awaitingSnapshotAcks += (mySeqNumber -> (sender(), request))
      replica ! Snapshot(key, value, mySeqNumber)
      setTimeout()

    case request @ SnapshotAck(_, seq: Long) =>
      log.debug("Replicator receive: SnapshotAck: request = {}", request)
      awaitingSnapshotAcks.get(seq).foreach {
        case (theSender: ActorRef, replicate: Replicate) =>
          theSender ! Replicated(replicate.key, replicate.id)
      }
      awaitingSnapshotAcks -= seq
      resetTimeout()

    case ReceiveTimeout =>
      log.debug("Replicator receive: ReceiveTimeout: resend Snapshot for {}", awaitingSnapshotAcks)
      awaitingSnapshotAcks.foreach {
        case (seqNumber, (_, request)) =>
          replica ! Snapshot(request.key, request.valueOption, seqNumber)
      }
      resetTimeout()

    case msg @ PersistFailed(persistMessage: Persist) =>
      log.debug("Replicator receive: PersistFailed: map to client ID msg = {}", msg)
      awaitingSnapshotAcks.get(persistMessage.id).foreach {
        case (theSender: ActorRef, replicate: Replicate) =>
          theSender ! Replica.PersistFailed(replicate.id)
      }

  } // Replicator Receive handler

} // class Replicator
