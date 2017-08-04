package chapter04

import akka.actor.ActorSystem
import cats.implicits._
import com.typesafe.config.ConfigFactory
import redis.RedisClient

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration._

object WatchTransaction extends App {

  def zpop(key: String): Future[String] = {
    val multi = client.watch(key)
    val exec = for {
      ds <- client.zrange[String](key, 0, 0)
      _ <- {
        multi.zrem(key, ds.head)
        multi.exec()
      }
    } yield ds.head
    exec.recoverWith {
      case _ => zpop(key)
    }
  }

  implicit val akkaSystem: ActorSystem = akka.actor.ActorSystem()

  val config = ConfigFactory.load()
  val hostname = config.getString("redis.hostname")
  val port = config.getInt("redis.port")

  val client = RedisClient(hostname, port)

  val r = for {
    _ <- client.flushall()
    _ <- client.zadd("presidents", (1732, "George Washington"))
    _ <- client.zadd("presidents", (1809, "Abrahm Lincoln"))
    _ <- client.zadd("presidents", (1858, "Theodors Roosevelt"))
    member <- zpop("presidents")
  } yield {
    println(s"The first president in the group is: $member")
  }

  Await.result(r, Duration.Inf)
  Await.result(akkaSystem.terminate(), Duration.Inf)
}
