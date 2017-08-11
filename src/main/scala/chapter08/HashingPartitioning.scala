package chapter08

import akka.actor.ActorSystem
import redis.RedisClient

case class HashingPartitioning(
  protected val clients: Seq[RedisClient]
) extends Partitioning {

  protected def getClient(key: String): RedisClient =
    clients(hashFunction(key) % clients.size)

  protected def hashFunction(key: String): Int =
    BigInt(java.security.MessageDigest.getInstance("MD5").digest(key.getBytes)).toInt
}

object HashingPartitioning extends App {
  implicit val actorSystem: ActorSystem = ActorSystem()

  val clients = (1 to 5).map(_ => RedisClient())

  val par = HashingPartitioning(clients)

  println(par.getClient("z"))

  actorSystem.terminate()
}

