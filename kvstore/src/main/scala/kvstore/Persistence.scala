package kvstore

import akka.actor.{Actor, Props}
import akka.event.LoggingReceive

import scala.util.Random

//noinspection SimplifyFactoryMethod
object Persistence {
  case class Persist(key: String, valueOption: Option[String], id: Long)
  case class Persisted(key: String, id: Long)

  class PersistenceException(val failedMessage: Persist) extends Exception("Persistence failure")

  def props(flaky: Boolean): Props = Props(classOf[Persistence], flaky)
}

class Persistence(flaky: Boolean) extends Actor {
  import Persistence._

  override def postRestart(reason: Throwable): Unit = reason match {
    case e: PersistenceException =>
      // Usually we would not retry a message that led to an exception.  In this case, however,
      // the message isn't the cause of the failure; the "flakiness" is the cause of the exception.
      self ! e.failedMessage
  }

  def receive: Receive = LoggingReceive {
    case request @ Persist(key, _, id) =>
      if (!flaky || Random.nextBoolean()) sender() ! Persisted(key, id)
      else throw new PersistenceException(request)
  }

}
