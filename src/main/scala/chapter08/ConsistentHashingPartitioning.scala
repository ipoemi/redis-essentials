package chapter08

import akka.actor.ActorSystem
import cats.Reducible.ops.toAllReducibleOps
import redis.RedisClient

case class ConsistentHashingPartitioning(
  protected val clients: Seq[RedisClient],
  protected val vnodes: Int = 256
) extends Partitioning {

  protected val ring: Map[Int, RedisClient] = setUpRing

  protected def getClient(key: String): RedisClient = {
    val ringHashes = ring.keys.toList.sorted
    val keyHash = hashFunction(key)
    val idx = ringHashes.indexWhere(_ >= keyHash) max 0
    ring(ringHashes(idx))
  }

  protected def hashFunction(key: String): Int = {
    val tagRegex = ".+\\{(.+)\\}".r
    val value = key match {
      case tagRegex(tag) => tag
      case _ => key
    }
    BigInt(java.security.MessageDigest.getInstance("MD5").digest(value.getBytes)).toInt
  }

  //noinspection MutatorLikeMethodIsParameterless
  protected def setUpRing: Map[Int, RedisClient] = {
    val kvs = for {
      i <- 0 until vnodes
      client <- clients
    } yield {
      val hash = hashFunction(s"${client.host}:$i")
      hash -> client
    }
    Map(kvs: _*)
  }

  def addClient(client: RedisClient): ConsistentHashingPartitioning =
    ConsistentHashingPartitioning(this.clients :+ client)

  def removeClient(client: RedisClient): ConsistentHashingPartitioning =
    ConsistentHashingPartitioning(this.clients.filterNot(_ == client))

}

object ConsistentHashingPartitioning extends App {
  implicit val actorSystem: ActorSystem = ActorSystem()

  val clients = (1 to 5).map(_ => RedisClient())

  val par = ConsistentHashingPartitioning(clients)

  println(par.getClient("z"))

  actorSystem.terminate()
}
