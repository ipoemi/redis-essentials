package chapter04

import akka.actor.ActorSystem
import akka.util.ByteString
import cats.implicits._
import com.typesafe.config.ConfigFactory
import redis.RedisClient

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object Publisher extends App {

  implicit val akkaSystem: ActorSystem = akka.actor.ActorSystem()

  val config = ConfigFactory.load()
  val hostname = config.getString("redis.hostname")
  val port = config.getInt("redis.port")

  val client = RedisClient(hostname, port)

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
