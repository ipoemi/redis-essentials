package chapter01

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import redis.{RedisBlockingClient, RedisClient}

import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object ProducerWorker extends App {
  implicit val akkaSystem: ActorSystem = akka.actor.ActorSystem()

  val config = ConfigFactory.load()
  val hostname = config.getString("redis.hostname")
  val port = config.getInt("redis.port")

  val client = RedisClient(hostname, port)
  val blockingClient = RedisBlockingClient("localhost", 6379)
  val logsQueue = RedisQueue("logs", client, blockingClient)
  val MAX = 5
  val r = (0 until 5).map { i =>
    logsQueue.push(s"Hello world #$i")
  }
  println(s"Created $MAX logs")
  Await.result(Future.sequence(r), Duration.Inf)
  Await.result(akkaSystem.terminate(), Duration.Inf)
}
