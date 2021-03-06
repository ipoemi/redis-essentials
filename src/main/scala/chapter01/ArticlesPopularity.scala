package chapter01

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import redis.RedisClient

import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import cats._
import cats.data._
import cats.implicits._

object ArticlesPopularity extends App {

  def upVote(client: RedisClient, id: String): Future[Long] = {
    val key = s"article:$id:votes"
    client.incr(key)
  }

  def downVote(client: RedisClient, id: String): Future[Long] = {
    val key = s"article:$id:votes"
    client.decr(key)
  }

  def showResults(client: RedisClient, id: String): Future[Unit] = {
    val headlineKey = s"article:$id:headline"
    val voteKey = s"article:$id:votes"
    client.mget[String](headlineKey, voteKey) map { xs =>
      for {
        headline <- xs.head
        votes <- xs(1)
      } println(s"The article $headline has $votes votes")
    }
  }

  implicit val akkaSystem: ActorSystem = akka.actor.ActorSystem()

  val config = ConfigFactory.load()
  val hostname = config.getString("redis.hostname")
  val port = config.getInt("redis.port")

  val client = RedisClient(hostname, port)

  val r = for {
    _ <- client.set("article:12345:headline", "Google Wants to Turn Your Clothes Into a Computer")
    _ <- client.set("article:10001:headline", "For Millennials, the End of the TV Viewing Party")
    _ <- client.set("article:60056:headline", "Alicia Vikander, Who Portrayed Denmark's Queen, Is Screen Royalty")
    _ <- upVote(client, "12345")
    _ <- upVote(client, "12345")
    _ <- upVote(client, "12345")
    _ <- upVote(client, "10001")
    _ <- upVote(client, "10001")
    _ <- downVote(client, "10001")
    _ <- upVote(client, "60056")
    _ <- showResults(client, "12345")
    _ <- showResults(client, "10001")
    _ <- showResults(client, "60056")
  } yield ()

  Await.ready(r, Duration.Inf)
  //Await.result(akkaSystem.terminate(), Duration.Inf)
  Await.result(akkaSystem.terminate(), Duration.Inf)
}
