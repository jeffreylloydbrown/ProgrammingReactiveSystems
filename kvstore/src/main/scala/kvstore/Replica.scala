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

package kvstore

import akka.actor.{Actor, ActorRef, OneForOneStrategy, PoisonPill, Props, SupervisorStrategy, Terminated}
import akka.event.LoggingReceive
import kvstore.Arbiter._
import akka.pattern.{ask, pipe}

import scala.concurrent.duration._
import akka.util.Timeout

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

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

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

  private val Gets: Receive = LoggingReceive {
    case Get(key: String, id: Long) =>
      sender() ! GetResult(key, kv.get(key), id)
  }

  private val Updates: Receive = LoggingReceive {
    case Insert(key: String, value: String, id: Long) =>
      kv += key->value
      sender() ! OperationAck(id)
    case Remove(key: String, id: Long) =>
      kv -= key
      sender() ! OperationAck(id)
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
      sender() ! SnapshotAck(key, seq)
      expectedSeqId = seq + 1
  }

  // Here is where Chain of Responsibility comes in handy!
  private val leader: Receive = Gets orElse Updates
  private val replica: Receive = Gets orElse Snapshots

  // leave this at the absolute bottom so it happens last
  arbiter ! Join
}

