package chapter01

import akka.actor.ActorSystem
import redis.RedisClient

import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import cats._
import cats.data._
import cats.implicits._

object HashVotingSystem extends App {
  def saveLink(id: String, author: String, title: String, link: String): Future[Boolean] =
    client.hmset(
      s"link:$id",
      Map(
        "author" -> author,
        "title" -> title,
        "link" -> link,
        "score" -> "0"
      )
    )

  def upVote(id: String): Future[Long] = client.hincrby(s"link:$id", "score", 1)

  def downVote(id: String): Future[Long] = client.hincrby(s"link:$id", "score", -1)

  def showDetails(id: String): Unit = {
    client.hgetall(s"link:$id").foreach { m =>
      println(s"Title: ${m.getOrElse("title", "")}")
      println(s"Author: ${m.getOrElse("author", "")}")
      println(s"Link: ${m.getOrElse("link", "")}")
      println(s"Score: ${m.getOrElse("score", "0")}")
      println("-----------------------------------")
    }
  }

  def getKeys(index: Int = 0): Future[Vector[String]] = {
    val curFt = client.scan(index)
    curFt.flatMap { cur =>
      if (cur.index == 0)
        Future { cur.data.toVector }
      else
        getKeys(cur.index) map (_ ++ cur.data.toVector)
    }
  }

  implicit val akkaSystem: ActorSystem = akka.actor.ActorSystem()

  val client = RedisClient("localhost", 6379)
  /*
  saveLink("123", "dayvson", "Maxwell Dayvson's Github page", "https://github.com/dayvson")
  upVote("123")
  upVote("123")

  saveLink("456", "hltbra", "Hugo Tavares's Github page", "https://github.com/hltbra")
  upVote("456")
  upVote("456")
  downVote("456")

  showDetails("123")
  showDetails("456")
  */

  Await.result(getKeys(0).map (_.foreach(println)), Duration.Inf)

  Await.result(akkaSystem.terminate(), Duration.Inf)
}
