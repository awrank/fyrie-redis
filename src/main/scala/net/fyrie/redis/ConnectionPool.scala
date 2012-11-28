package net.fyrie
package redis

import messages.{ RequestClient, ReleaseClient, Disconnect }
import akka.actor._
import concurrent.{ Promise, Future }
import collection.immutable.Queue
import util.{ Success, Failure }

private[redis] class ConnectionPool(initialSize: Range, factory: (ActorRefFactory) ⇒ RedisClientPoolWorker) extends Actor {
  var ready: List[RedisClientPoolWorker] = Nil
  var active: Set[RedisClientPoolWorker] = Set.empty
  var limit: Range = initialSize
  var size: Int = 0
  var queue: Queue[Promise[RedisClientPoolWorker]] = Queue.empty

  def receive = {
    case RequestClient(promise) ⇒
      if (active.size >= limit.max)
        queue = queue enqueue promise
      else {
        val client = ready match {
          case h :: t ⇒
            ready = t
            h
          case _ ⇒
            size += 1
            factory(context)
        }
        active += client
        promise complete Success(client)
      }
    case ReleaseClient(client) ⇒
      if (active.size > limit.max) {
        killClient(client)
      } else if (queue.nonEmpty) {
        val (promise, rest) = queue.dequeue
        promise complete Success(client)
        queue = rest
      } else if ((size - active.size) == limit.min) {
        killClient(client)
      } else {
        ready ::= client
        active -= client
      }
  }

  def killClient(client: RedisClientPoolWorker) {
    client.disconnect
    active -= client
    size -= 1
  }

  override def postStop {
    queue foreach (_ complete Failure(RedisConnectionException("Connection pool shutting down")))
  }

}
