package chapter01

import redis.{RedisClient, RedisBlockingClient}
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object ProducerWorker extends App {
  implicit val akkaSystem = akka.actor.ActorSystem()

  val client = RedisClient("localhost", 6379)
  val blockingClient = RedisBlockingClient("localhost", 6379)
  val logsQueue = RedisQueue("logs", client, blockingClient)
  val MAX = 5
  val r = (0 until 5).map { i =>
    logsQueue.push(s"Hello world #${i}")
  }
  println(s"Created ${MAX} logs")
  Await.result(Future.sequence(r), Duration.Inf)
  Await.result(akkaSystem.terminate(), Duration.Inf)
}
