package kvstore

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Terminated}
import akka.event.LoggingReceive

import scala.concurrent.duration._

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor with ActorLogging {
  import Replicator._
  import context.dispatcher
  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var awaitingSnapshotAcks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]

  private val sequenceGenerator = new LongGenerator()


  private def Replication: Receive = LoggingReceive {
    case request @ Replicate(key, valueOption, _) =>
      val seq = sequenceGenerator.next()
      awaitingSnapshotAcks +=   seq -> (sender(), request)
      replica ! Snapshot(key, valueOption, seq)

    case SnapshotAck(_, seq) =>
      awaitingSnapshotAcks.get(seq).foreach {
        case (originator, replicate) =>
          originator ! Replicated(replicate.key, replicate.id)
      }
      awaitingSnapshotAcks -= seq // don't modify the container INSIDE the loop processing it!

  }

  // If the replica goes away, so do we.
  private def ReplicaTerminated: Receive = LoggingReceive {
    case Terminated(`replica`) =>
      context.stop(self)
  }
  context.watch(replica)


  def receive: Receive = Replication orElse ReplicaTerminated

}
