package chapter04

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import redis.RedisClient

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object ZpopLuaEvalsha extends App {

  val luaScript =
    """
      |return "Lua script using EVALSHA"
    """.stripMargin

  implicit val akkaSystem: ActorSystem = akka.actor.ActorSystem()

  val config = ConfigFactory.load()
  val hostname = config.getString("redis.hostname")
  val port = config.getInt("redis.port")

  val client = RedisClient(hostname, port)

  val r = for {
    _ <- client.flushall()
    scriptId <- client.scriptLoad(luaScript)
    msg <- client.evalsha[String](scriptId)
  } yield {
    println(msg)
  }

  Await.result(r, Duration.Inf)
  Await.result(akkaSystem.terminate(), Duration.Inf)
}
