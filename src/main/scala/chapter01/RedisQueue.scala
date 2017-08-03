package chapter01

import akka.actor.ActorSystem
import redis.RedisClient
import redis.RedisBlockingClient

import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import cats._
import cats.data._
import cats.implicits._

case class RedisQueue(name: String, client: RedisClient, blockingClient: RedisBlockingClient) {
  val queueKey = s"queue:$name"

  def size: Future[Long] = {
    client.llen(queueKey)
  }

  def push(value: String): Future[Long] = {
    client.lpush(queueKey, value)
  }

  def pop: Future[Option[(String, String)]] = {
    OptionT(blockingClient.brpop[String](List(queueKey))).map { value =>
      (value._1, value._2)
    }.value
  }
}

object RedisQueue extends App {
  implicit val akkaSystem: ActorSystem = akka.actor.ActorSystem()
  val client = RedisClient("localhost", 6379)
  val blockingClient = RedisBlockingClient("localhost", 6379)
  val queue1 = RedisQueue("queue1", client, blockingClient)
  println(queue1.size.foreach(println(_)))
  println(queue1.push("data1"))
  println(queue1.push("data2"))
  println(queue1.pop.foreach(println(_)))
  Await.result(akkaSystem.terminate(), Duration.Inf)
}
