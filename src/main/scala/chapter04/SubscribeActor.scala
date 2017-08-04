package chapter04

import akka.actor.{ActorSystem, Props}
import com.github.nscala_time.time.Imports.DateTime
import com.typesafe.config.ConfigFactory
import redis.actors.RedisSubscriberActor
import redis.api.pubsub._

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class SubscribeActor(hostname: String, port: Int, channels: Seq[String] = Nil, patterns: Seq[String] = Nil) extends
  RedisSubscriberActor(
    new java.net.InetSocketAddress(hostname, port),
    channels,
    patterns,
    onConnectStatus = connected => { println(s"connected: $connected") }
  ) {

  import SubscribeActor.Command

  def onMessage(message: Message): Unit = message match {
    case Message(_, bs) =>
      val cs = bs.decodeString("utf-8")
      Command.get(cs) match {
        case Some(cmd) => cmd()
        case None => println(s"$cs is not command")
      }
  }

  def onPMessage(pmessage: PMessage) {
    println(s"pattern message received: $pmessage")
  }
}

object SubscribeActor extends App {
  val Command = Map(
    "DATE" -> (() => println(s"Date ${ DateTime.now }")),
    "PING" -> (() => println("PONG")),
    "HOSTNAME" -> (() => println(s"HOSTNAME ${ java.net.InetAddress.getLocalHost.getHostName }"))
  )

  implicit val akkaSystem: ActorSystem = akka.actor.ActorSystem()

  val channel = Seq("channel-1")
  val pattern = Seq("*")

  val config = ConfigFactory.load()
  val hostname = config.getString("redis.hostname")
  val port = config.getInt("redis.port")

  val actor = akkaSystem.actorOf(Props(classOf[SubscribeActor], hostname, port, channel, pattern))

  akkaSystem.scheduler.scheduleOnce(20 seconds)(Await.result(akkaSystem.terminate(), Duration.Inf))
}
