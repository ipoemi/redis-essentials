package chapter02

import cats.data._
import cats.implicits._
import redis.RedisClient

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

object MetricsBitmap extends App {

  def makeDateKey(date: String): String = s"visits:daily:$date"

  def storeDailyVisit(date: String, userId: String): Future[Boolean] = {
    val key = makeDateKey(date)
    client.setbit(key, userId.toLong, true) map { r =>
      println(s"User $userId visited on $date")
      r
    }
  }

  def countVisits(date: String): Future[Long] = {
    val key = makeDateKey(date)
    client.bitcount(key) map { r =>
      println(s"$date had $r visits.")
      r
    }
  }

  private def getOffsetAndValues(byte: Byte, byteIdx: Long): Vector[(Long, Boolean)] =
    (0 to 7).reverse.toVector map { bitIdx =>
      val value = if ((byte >> bitIdx & 1) == 1) true else false
      val offset = byteIdx * 8 + (7 - bitIdx)
      (offset, value)
    }

  def showUserIdsFromVisit(date: String): Future[Unit] = {
    val key = makeDateKey(date)
    client.get(key) map { byteStringOpt =>
      byteStringOpt match {
        case None => ()
        case Some(byteString) =>
          val bytes = byteString.toVector
          val userIds = bytes.zipWithIndex flatMap { bi =>
            getOffsetAndValues(bi._1, bi._2).filter(_._2).map(_._1)
          }
          println(s"Users $userIds visited on $date")
      }
    }
  }

  implicit val akkaSystem = akka.actor.ActorSystem()

  val client = RedisClient("localhost", 6379)
  client.del(makeDateKey("2015-01-01"))

  val r = for {
    _ <- storeDailyVisit("2015-01-01", "1")
    _ <- storeDailyVisit("2015-01-01", "2")
    _ <- storeDailyVisit("2015-01-01", "10")
    _ <- storeDailyVisit("2015-01-01", "50000")

    _ <- countVisits("2015-01-01")

    _ <- showUserIdsFromVisit("2015-01-01")
  } yield ()

  Await.result(r, Duration.Inf)
  Await.result(akkaSystem.terminate(), Duration.Inf)
}
