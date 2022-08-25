package kvstore

import akka.actor.{Actor, ActorLogging, ActorRef, Props, ReceiveTimeout}
import akka.event.LoggingReceive

import scala.concurrent.duration._

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)

  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

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
    case request @ Replicate(key: String, value: Option[String], _) =>
      val mySeqNumber = nextSeq()
      awaitingSnapshotAcks += (mySeqNumber -> (sender(), request))
      replica ! Snapshot(key, value, mySeqNumber)
      setTimeout()

    case SnapshotAck(_, seq: Long) =>
      awaitingSnapshotAcks.get(seq).foreach {
        case (theSender: ActorRef, replicate: Replicate) =>
          theSender ! Replicated(replicate.key, replicate.id)
      }
      awaitingSnapshotAcks -= seq
      resetTimeout()

    case ReceiveTimeout =>
      awaitingSnapshotAcks.foreach {
        case (seqNumber, (_, request)) =>
          replica ! Snapshot(request.key, request.valueOption, seqNumber)
      }
      resetTimeout()

  } // Replicator Receive handler

} // class Replicator
