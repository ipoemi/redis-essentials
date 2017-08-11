package chapter08

import akka.actor.ActorSystem
import redis.RedisClient

case class RangePartitioning(
  protected val clients: Seq[RedisClient]
) extends Partitioning {

  protected def getClient(key: String): RedisClient = {
    val possibleValues = "0123456789abcdefghijklmnopqrstuvwxyz"
    val rangeSize = possibleValues.length / clients.size
    val clientIndex =
      possibleValues
        .sliding(rangeSize, rangeSize)
        .indexWhere(_.contains(key.head.toLower))
        .max(0) % clients.size
    clients(clientIndex)
  }
}

object RangePartitioning extends App {
  implicit val actorSystem: ActorSystem = ActorSystem()

  val clients = (1 to 5).map(_ => RedisClient())

  val par = RangePartitioning(clients)

  println(par.getClient("z"))

  actorSystem.terminate()
}
