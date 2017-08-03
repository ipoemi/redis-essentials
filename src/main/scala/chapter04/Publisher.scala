package chapter04

import akka.util.ByteString
import cats.implicits._
import redis.RedisClient

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object Publisher extends App {

  implicit val akkaSystem = akka.actor.ActorSystem()

  val client = RedisClient("localhost", 6379)

  val channel = "channel-1"

  val r = for {
    _ <- client.publish(channel, "NO!!")
    _ <- client.publish(channel, "DATE")
    _ <- client.publish(channel, "HOSTNAME")
    _ <- client.publish(channel, "PING")
  } yield ()

  Await.result(r, Duration.Inf)
  Await.result(akkaSystem.terminate(), Duration.Inf)
}
