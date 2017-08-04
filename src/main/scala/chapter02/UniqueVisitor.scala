package chapter02

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import redis.RedisClient

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object UniqueVisitor extends App {

  def makeKey(date: String): String = s"visits:$date"

  def addVisit(date: String, userId: String): Future[Long] = {
    val key = makeKey(date)
    client.pfadd(key, userId)
  }

  def count(dates: Seq[String]): Future[Long] = {
    val keys = dates map makeKey
    client.pfcount(keys: _*) map { r =>
      println(s"Dates ${dates.mkString(", ")} had $r visits")
      r
    }
  }

  def aggregateDate(date: String): Future[Boolean] = {
    val destKey = makeKey(date)
    val srcKeys = (0 to 23) map (t => makeKey(date + "T" + t.toString))
    client.pfmerge(destKey, srcKeys: _*) map { r =>
      println(s"Aggregated date $date")
      r
    }
  }

  implicit val akkaSystem: ActorSystem = akka.actor.ActorSystem()

  val config = ConfigFactory.load()
  val hostname = config.getString("redis.hostname")
  val port = config.getInt("redis.port")

  val client = RedisClient(hostname, port)

  val MaxUser = 200
  val TotalVisits = 1000

  val r = for {
    _ <- Future.sequence((0 until TotalVisits) map { _ =>
      val userId = s"user_${Math.floor(1 + Math.random() * MaxUser).toInt}"
      val hour = Math.floor(Math.random() * 23).toInt
      addVisit(s"2015-01-01T$hour", userId)
    })

    _ <- count(List("2015-01-01T0"))
    _ <- count(List("2015-01-01T5", "2015-01-01T6", "2015-01-01T7"))

    _ <- aggregateDate("2015-01-01")
    _ <- count(List("2015-01-01"))
  } yield ()


  Await.result(r, Duration.Inf)
  Await.result(akkaSystem.terminate(), Duration.Inf)
}
