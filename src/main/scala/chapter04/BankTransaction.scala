package chapter04

import akka.actor.ActorSystem
import akka.util.ByteString
import com.github.nscala_time.time.Imports.DateTime
import com.typesafe.config.ConfigFactory
import cats.implicits._
import redis._
import redis.api.pubsub.Message
import redis.commands.TransactionBuilder

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Try

object BankTransaction extends App {

  sealed trait TransactionResult
  final case class Success(fromBalance: Long, toBalance: Long) extends TransactionResult
  final case class Failure(reason: String) extends TransactionResult

  implicit val longByteStringFormatter: ByteStringFormatter[Long] = new ByteStringFormatter[Long] {
    def deserialize(bs: ByteString): Long = Try(bs.decodeString("utf-8").toLong).toOption.getOrElse(0L)
    def serialize(data: Long): ByteString = ByteString.fromString(data.toString)
  }

  def transfer(from: String, to: String, value: Long): Future[TransactionResult] = {
    val balanceFt = client.get[Long](from) map (_.getOrElse(0L))
    balanceFt flatMap { balance =>
      val multi = client.multi()
      val fromBalanceAndToBalanceFt = (multi.decrby(from, value) |@| multi.incrby(to, value)) map ((_, _))
      if (balance >= value) {
        multi.exec()
        fromBalanceAndToBalanceFt map {
          case (fromBalance, toBalance) => Success(fromBalance, toBalance)
        }
      } else {
        multi.discard()
        Failure("Insufficient funds").pure[Future]
      }
    }
  }

  implicit val akkaSystem: ActorSystem = akka.actor.ActorSystem()

  val config = ConfigFactory.load()
  val hostname = config.getString("redis.hostname")
  val port = config.getInt("redis.port")

  val client = RedisClient(hostname, port)

  val r = for {
    _ <- client.mset[Long](Map("max:checkings" -> 100, "hugo:checkings" -> 100))
    _ <- Future {
      println("Max checkings: 100")
      println("Hugo checkings: 100")
    }
    ret <- transfer("max:checkings", "hugo:checkings", 40)
  } yield ret match {
    case Success(from, _) =>
      println("Transferred 40 from Max to Hugo")
      println(s"Max balance: $from")
    case Failure(msg) =>
      println(msg)
  }

  Await.result(r, Duration.Inf)
  Await.result(akkaSystem.terminate(), Duration.Inf)
}
