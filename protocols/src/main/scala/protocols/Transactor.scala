package protocols

import akka.actor.typed._
import akka.actor.typed.scaladsl._

import scala.concurrent.duration._

object Transactor {

  sealed trait PrivateCommand[T] extends Product with Serializable
  final case class Committed[T](session: ActorRef[Session[T]], value: T) extends PrivateCommand[T]
  final case class RolledBack[T](session: ActorRef[Session[T]]) extends PrivateCommand[T]

  sealed trait Command[T] extends PrivateCommand[T]
  final case class Begin[T](replyTo: ActorRef[ActorRef[Session[T]]]) extends Command[T]

  sealed trait Session[T] extends Product with Serializable
  final case class Extract[T, U](f: T => U, replyTo: ActorRef[U]) extends Session[T]
  final case class Modify[T, U](f: T => T, id: Long, reply: U, replyTo: ActorRef[U]) extends Session[T]
  final case class Commit[T, U](reply: U, replyTo: ActorRef[U]) extends Session[T]
  final case class Rollback[T]() extends Session[T]

  /**
    * @return A behavior that accepts public [[Command]] messages. The behavior
    *         should be wrapped in a [[SelectiveReceive]] decorator (with a capacity
    *         of 30 messages) so that beginning new sessions while there is already
    *         a currently running session is deferred to the point where the current
    *         session is terminated.
    * @param value Initial value of the transactor
    * @param sessionTimeout Delay before rolling back the pending modifications and
    *                       terminating the session
    */
//  def apply[T](value: T, sessionTimeout: FiniteDuration): Behavior[Command[T]] = SelectiveReceive(30,
//    idle(value, sessionTimeout) match {
//      case i: Behavior[PrivateCommand[T]] => i.asInstanceOf[Behavior[Command[T]]]
//    }
//  )
  // Jump to Behavior.narrow to see why we have to do this here.
  def apply[T](value: T, sessionTimeout: FiniteDuration): Behavior[Command[T]] =
    SelectiveReceive(30, idle(value, sessionTimeout).narrow[Command[T]])

  /**
    * @return A behavior that defines how to react to any [[PrivateCommand]] when the transactor
    *         has no currently running session.
    *         [[Committed]] and [[RolledBack]] messages should be ignored, and a [[Begin]] message
    *         should create a new session.
    *
    * @param value Value of the transactor
    * @param sessionTimeout Delay before rolling back the pending modifications and
    *                       terminating the session
    *
    * Note: To implement the timeout you have to use `ctx.scheduleOnce` instead of `Behaviors.withTimers`, due
    *       to a current limitation of Akka: https://github.com/akka/akka/issues/24686
    *
    * Hints:
    *   - When a [[Begin]] message is received, an anonymous child actor handling the session should be spawned,
    *   - In case the child actor is terminated, the session should be rolled back,
    *   - When `sessionTimeout` expires, the session should be rolled back,
    *   - After a session is started, the next behavior should be [[inSession]],
    *   - Messages other than [[Begin]] should not change the behavior.
    */
  private def idle[T](value: T, sessionTimeout: FiniteDuration): Behavior[PrivateCommand[T]] =
    Behaviors.receive[PrivateCommand[T]] {
      case (ctx, Begin(replyTo)) =>
        val session = ctx.spawnAnonymous(
          sessionHandler(value, ctx.self, Set.empty))
        ctx.watchWith(session, RolledBack(session))
        ctx.scheduleOnce(sessionTimeout, ctx.self, RolledBack(session))
        replyTo ! session
        inSession(value, sessionTimeout, session)
      case _ =>
        Behaviors.ignore
    }

  /**
    * @return A behavior that defines how to react to [[PrivateCommand]] messages when the transactor has
    *         a running session.
    *         [[Committed]] and [[RolledBack]] messages should commit and rollback the session, respectively.
    *         [[Begin]] messages should be unhandled (they will be handled by the [[SelectiveReceive]] decorator).
    *
    * @param rollbackValue Value to rollback to
    * @param sessionTimeout Timeout to use for the next session
    * @param sessionRef Reference to the child [[Session]] actor
    */
  private def inSession[T](rollbackValue: T, sessionTimeout: FiniteDuration, sessionRef: ActorRef[Session[T]]): Behavior[PrivateCommand[T]] =
    Behaviors.receive {
      case (_, Committed(_, value)) =>
        idle(value, sessionTimeout)
      case (ctx, RolledBack(session)) =>
        ctx.stop(session)
        idle(rollbackValue, sessionTimeout)
      case (_, Begin(_)) =>
        Behaviors.unhandled
    }

  /**
    * @return A behavior handling [[Session]] messages. See in the instructions
    *         the precise semantics that each message should have.
    *
    * @param currentValue The session’s current value
    * @param commit Parent actor reference, to send the [[Committed]] message to
    * @param done Set of already applied [[Modify]] messages
    */
  private def sessionHandler[T](currentValue: T, commit: ActorRef[Committed[T]], done: Set[Long]): Behavior[Session[T]] =
    Behaviors.receive[Session[T]] {
      case (_, Extract(f, replyTo)) =>
        replyTo ! f(currentValue)
        Behaviors.same
      case (_, Modify(_, id, reply, replyTo)) if done.contains(id) =>
        replyTo ! reply
        Behaviors.same
      case (_, Modify(f, id, reply, replyTo)) =>
        replyTo ! reply
        sessionHandler(f(currentValue), commit, done + id)
      case (ctx, Commit(reply, replyTo)) =>
        replyTo ! reply
        commit ! Committed(ctx.self, currentValue)
        Behaviors.stopped
      case (_, Rollback()) =>
        Behaviors.stopped
    }

}
