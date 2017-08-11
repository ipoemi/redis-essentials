package chapter08

import redis.{ByteStringDeserializer, ByteStringSerializer, RedisClient}

import scala.concurrent.Future

trait Partitioning {
  protected def clients: Seq[RedisClient]

  protected def getClient(key: String): RedisClient

  def set[A](key: String, value: A)(implicit bss: ByteStringSerializer[A]): Future[Boolean] = {
    val client = getClient(key)
    client.set[A](key, value)(bss)
  }

  def get[A](key: String, value: A)(implicit bds: ByteStringDeserializer[A]): Future[Option[A]] = {
    val client = getClient(key)
    client.get[A](key)(bds)
  }
}
