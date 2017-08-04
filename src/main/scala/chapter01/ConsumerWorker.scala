package chapter01

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import redis.RedisClient
import redis.RedisBlockingClient

import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object ConsumerWorker extends App {
  implicit val akkaSystem: ActorSystem = akka.actor.ActorSystem()

  val config = ConfigFactory.load()
  val hostname = config.getString("redis.hostname")
  val port = config.getInt("redis.port")

  val client = RedisClient(hostname, port)
  val blockingClient = RedisBlockingClient(hostname, port)
  val logsQueue = RedisQueue("logs", client, blockingClient)

  def logMessages(): Unit = {
    val result = logsQueue.pop
    val r = Await.result(result, Duration.Inf)
    r.foreach { v =>
      val message = v._2
      println(s"[consumer] Got log: $message")

      logsQueue.size.map { s =>
        println(s"$s logs left")
      }
    }
    logMessages()
  }

  logMessages()

  Await.result(akkaSystem.terminate(), Duration.Inf)
}
