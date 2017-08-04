package chapter06

import akka.actor.ActorSystem
import cats.implicits._
import com.typesafe.config.ConfigFactory
import redis.RedisClient

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object BenchmarkSet extends App {

  implicit val akkaSystem: ActorSystem = akka.actor.ActorSystem()

  val config = ConfigFactory.load()
  val hostname = config.getString("redis.hostname")
  val port = config.getInt("redis.port")

  val client = RedisClient(hostname, port)

  val MaxUsers = 100000
  val MaxDeals = 12
  val MaxDealId = 10000

  client.flushall()

  val bm = ((0 until MaxUsers) map { i =>
    val multi = client.multi()
    (0 until MaxDeals) map { j =>
      multi.sadd(s"set:user:$i", MaxDealId - j)
    }
    multi.exec()
  }).toVector.sequenceU

  val r = for {
    _ <- bm
    infos <- client.info("memory")
  } yield println(infos)

  Await.result(r, Duration.Inf)
  Await.result(akkaSystem.terminate(), Duration.Inf)
}
