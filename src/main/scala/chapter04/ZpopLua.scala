package chapter04

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import redis.RedisClient

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object ZpopLua extends App {

  val luaScript =
    """
      |local elements = redis.call("ZRANGE", KEYS[1], 0, 0)
      |redis.call("ZREM", KEYS[1], elements[1])
      |return elements[1]
    """.stripMargin

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
    member <- client.eval[String](luaScript, Seq("presidents"))
  } yield {
    println(s"The first president in the group is: $member")
  }

  Await.result(r, Duration.Inf)
  Await.result(akkaSystem.terminate(), Duration.Inf)
}
