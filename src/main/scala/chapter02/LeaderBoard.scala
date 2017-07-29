package chapter02

import redis.RedisClient
import redis.RedisBlockingClient
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import cats._, cats.data._, cats.implicits._

object LeaderBoard extends App {

  implicit val akkaSystem = akka.actor.ActorSystem()

  val client = RedisClient("localhost", 6379)

  case class User(rank: Long, score: Double, userName: String)

  case class LeaderBoard(key: String) {
    def addUser(username: String, score: Double): Future[Long] = {
      client.zadd(key, (score, username)) map { r =>
        println(s"User $username added to the leaderboard!");
        r
      }
    }

    def removeUser(username: String): Future[Long] = {
      client.zrem(key, username) map { r =>
        println(s"User $username removed successfully!")
        r
      }
    }

    def getUserScoreAndRank(username: String): Future[(Option[Double], Option[Long])] = {
      val score = client.zscore(key, username)
      val rank = client.zrevrank(key, username)
      (score |@| rank).map { (s, r) =>
        println(s"\nDetails of $username:")
        println(s"Score: $s, Rank: #${r.map(_ + 1)}")
        (s, r)
      }
    }

    def showTopUsers(quantity: Long): Future[Unit] = {
      client.zrevrangeWithscores(key, 0, quantity - 1) map  { xs =>
        println(s"\nTop $quantity users:")
        xs.zipWithIndex.foreach { case (x, i) =>
          println(s"#${i + 1} User: ${x._1.decodeString("utf-8")}, score: ${x._2}")
        }
      }
    }

    def getUsersAroundUser(username: String, quantity: Long): Future[Option[Seq[User]]] = {
      (for {
        rank <- OptionT(client.zrevrank(key, username))
        startOffset = Math.floor(rank - (quantity / 2)).toInt max 0
        endOffset = startOffset + quantity - 1
        rets <- OptionT(client.zrevrangeWithscores(key, startOffset, endOffset).map(_.some))
      } yield {
        rets.zipWithIndex.map {
          case (x, i) => User(startOffset + i + 1, x._2, x._1.decodeString("utf-8"))
        }
      }).value
    }

  }

  val leaderBoard = LeaderBoard("game-score")
  val r = for {
    _ <- leaderBoard.addUser("Arthur", 70)
    _ <- leaderBoard.addUser("KC", 20)
    _ <- leaderBoard.addUser("Maxwell", 10)
    _ <- leaderBoard.addUser("Patrik", 30)
    _ <- leaderBoard.addUser("Ana", 60)
    _ <- leaderBoard.addUser("Felipe", 40)
    _ <- leaderBoard.addUser("Renata", 50)
    _ <- leaderBoard.addUser("Hugo", 80)
    _ <- leaderBoard.removeUser("Arthur")
    _ <- leaderBoard.getUserScoreAndRank("Maxwell")
    _ <- leaderBoard.showTopUsers(3)
    usersOpt <- leaderBoard.getUsersAroundUser("Felipe", 5)
  } yield  usersOpt map { users =>
    println("\nUsers around Felipe:")
    users.foreach { user =>
      println(s"#${user.rank} User: ${user.userName}, score: ${user.score}")
    }
  }


  Await.result(r, Duration.Inf)
  Await.result(akkaSystem.terminate(), Duration.Inf)
}
