package chapter02

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import redis.RedisClient
import redis.RedisBlockingClient

import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import cats._
import cats.data._
import cats.implicits._

object DealMetrics extends App {
  def markDealAsSent(dealId: String, userId: String): Future[Long] = {
    client.sadd(dealId, userId)
  }

  def sendDealIfNotSent(dealId: String, userId: String): Future[Long] = {
    client.sismember(dealId, userId) flatMap { isMember =>
      if (isMember) {
        println(s"Deal $dealId was already sent to user $userId")
        Future.successful(0)
      } else {
        println(s"Sending $dealId to user $userId")
        markDealAsSent(dealId, userId)
      }
    }
  }

  def showUsersThatReceivedAllDeals(dealIds: List[String]): Future[Unit] = dealIds match {
    case Nil => Future.successful(())
    case h :: t =>
      client.sinter[String](h, t: _*) map { users =>
        println(s"$users received all of the deals: $dealIds")
      }
  }

  def showUsersThatReceivedAtLeastOneOfTheDeals(dealIds: List[String]): Future[Unit] = dealIds match {
    case Nil => Future.successful(())
    case h :: t =>
      client.sunion[String](h, t: _*) map { users =>
        println(s"$users received all of the deals: $dealIds")
      }
  }

  implicit val akkaSystem: ActorSystem = akka.actor.ActorSystem()
  val config = ConfigFactory.load()
  val hostname = config.getString("redis.hostname")
  val port = config.getInt("redis.port")

  val client = RedisClient(hostname, port)

  val r = for {
    _ <- markDealAsSent("deal:1", "user:1")
    _ <- markDealAsSent("deal:1", "user:2")
    _ <- markDealAsSent("deal:2", "user:1")
    _ <- markDealAsSent("deal:2", "user:3")

    _ <- sendDealIfNotSent("deal:1", "user:1")
    _ <- sendDealIfNotSent("deal:1", "user:2")
    _ <- sendDealIfNotSent("deal:1", "user:3")

    _ <- showUsersThatReceivedAllDeals(List("deal:1", "deal:2"))
    _ <- showUsersThatReceivedAtLeastOneOfTheDeals(List("deal:1", "deal:2"))
  } yield ()

  Await.result(r, Duration.Inf)
  Await.result(akkaSystem.terminate(), Duration.Inf)
}
