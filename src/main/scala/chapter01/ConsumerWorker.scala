package chapter01

import redis.RedisClient
import redis.RedisBlockingClient
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object ConsumerWorker extends App {
  implicit val akkaSystem = akka.actor.ActorSystem()

  val client = RedisClient("localhost", 6379)
  val blockingClient = RedisBlockingClient("localhost", 6379)
  val logsQueue = RedisQueue("logs", client, blockingClient)

  def logMessages(): Unit = {
    val result = logsQueue.pop
    val r = Await.result(result, Duration.Inf)
    r.foreach { v =>
      val queueName = v._1
      val message = v._2
      println(s"[consumer] Got log: ${message}")

      logsQueue.size.map { s =>
        println(s"$s logs left")
      }
    }
    logMessages()
  }

  logMessages()

  Await.result(akkaSystem.terminate(), Duration.Inf)
}
