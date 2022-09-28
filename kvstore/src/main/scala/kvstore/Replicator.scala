package kvstore

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Terminated}
import akka.event.LoggingReceive

import scala.concurrent.duration._

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)

  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)
  private case object ResendSnapshots

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
} // object Replicator

class Replicator(val replica: ActorRef) extends Actor with ActorLogging {
  import Replicator._
  import context.dispatcher

  /** State holds the state information for this actor.  Use it with context.becomes()
    * to make actor state changes between messages.
    *
    * @param awaitingSnapshotAcks map for sequence number to pair of originator and replicate request
    */
  private case class State(awaitingSnapshotAcks: Map[Long, (ActorRef, Replicate)])

  private val sequenceGenerator = new LongGenerator()


  private def Messages(state: State): Receive = LoggingReceive {
    case request @ Replicate(key, valueOption, _) =>
      val seq = sequenceGenerator.next()
      replica ! Snapshot(key, valueOption, seq)
      context.become(Messages(state.copy(
        state.awaitingSnapshotAcks + (seq -> (sender(), request)))))

    case SnapshotAck(_, seq) =>
      state.awaitingSnapshotAcks.get(seq).foreach {
        case (originator, replicate) =>
          originator ! Replicated(replicate.key, replicate.id)
      }
      // don't modify the container INSIDE the loop processing it!
      context.become(Messages(state.copy(state.awaitingSnapshotAcks - seq)))

    case ResendSnapshots =>
      state.awaitingSnapshotAcks.foreach {
        case (seq, (_, replicate)) =>
          replica ! Snapshot(replicate.key, replicate.valueOption, seq)
      }

    case Terminated(`replica`) =>
      context.stop(self)
  }

  def receive: Receive = Messages(State(awaitingSnapshotAcks = Map.empty))

  // If the replica goes away, so do we.
  context.watch(replica)

  // Assignment says to retry snapshots every 100 ms
  context.system.scheduler.scheduleAtFixedRate(100.milliseconds, 100.milliseconds,
    self, ResendSnapshots)

} // class Replicator
