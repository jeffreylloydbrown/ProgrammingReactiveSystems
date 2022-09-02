package kvstore

import akka.actor.{Actor, ActorLogging, ActorRef, Props, ReceiveTimeout}
import akka.event.LoggingReceive
import kvstore.Persistence.Persist
import kvstore.Replica.OperationFailed

import scala.concurrent.duration._

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)

  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  case class PersistFailed(persistMessage: Persist)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))

  private val SnapshotTimeout = 95.milliseconds
  private val MaxTimeUntilOperationFailed: FiniteDuration = 1000.milliseconds
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

  def receive: Receive = normal(MaxTimeUntilOperationFailed)

  def normal(timeLeftUntilFailure: FiniteDuration): Receive = LoggingReceive {
    case request @ Replicate(key: String, value: Option[String], _: Long) =>
      log.debug("Replicator normal: Replicate: request = {}", request)
      val mySeqNumber = nextSeq()
      awaitingSnapshotAcks += (mySeqNumber -> (sender(), request))
      replica ! Snapshot(key, value, mySeqNumber)
      setTimeout()

    case request @ SnapshotAck(_, seq: Long) =>
      log.debug("Replicator normal: SnapshotAck: request = {}", request)
      awaitingSnapshotAcks.get(seq).foreach {
        case (theSender: ActorRef, replicate: Replicate) =>
          theSender ! Replicated(replicate.key, replicate.id)
      }
      awaitingSnapshotAcks -= seq
      resetTimeout()

    case ReceiveTimeout if timeLeftUntilFailure > 0.milliseconds =>
      log.debug("Replicator normal: ReceiveTimeout: {} remaining, resend Snapshot for {}",
        timeLeftUntilFailure, awaitingSnapshotAcks)
      awaitingSnapshotAcks.foreach {
        case (seqNumber, (_, request)) =>
          replica ! Snapshot(request.key, request.valueOption, seqNumber)
      }
      resetTimeout()
      // in this case, DO NOT PUSH context.
      context.become(normal(timeLeftUntilFailure - SnapshotTimeout))

    case ReceiveTimeout if timeLeftUntilFailure <= 0.milliseconds =>
      log.debug("Replicator normal: ReceiveTimeout no time left")
      // TODO is failing ALL of them really correct?  Testing will tell....
      awaitingSnapshotAcks.foreach {
        case (seqNumber, (theSender, request)) =>
          log.debug("Replicator normal: ReceiveTimeout no time left: seq {}: send OperationFailed({}) to {}",
            seqNumber, request.id, theSender)
          theSender ! OperationFailed(request.id)
      }
      awaitingSnapshotAcks = Map.empty[Long, (ActorRef, Replicate)]
      resetTimeout()
      context.become(receive)

    case msg @ PersistFailed(persistMessage: Persist) =>
      log.debug("Replicator normal: PersistFailed: map to client ID msg = {}", msg)
      awaitingSnapshotAcks.get(persistMessage.id).foreach {
        case (theSender: ActorRef, replicate: Replicate) =>
          theSender ! OperationFailed(replicate.id)
      }

  } // Replicator Receive handler

} // class Replicator
