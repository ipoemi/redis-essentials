package chapter04

import akka.actor.Props
import com.github.nscala_time.time.Imports.DateTime
import redis.actors.RedisSubscriberActor
import redis.api.pubsub._

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class SubscribeActor(channels: Seq[String] = Nil, patterns: Seq[String] = Nil) extends
  RedisSubscriberActor(
    new java.net.InetSocketAddress("localhost", 6379),
    channels,
    patterns,
    onConnectStatus = connected => { println(s"connected: $connected") }
  ) {

  import SubscribeActor.Command

  def onMessage(message: Message) = message match {
    case Message(ch, bs) => {
      val cs = bs.decodeString("utf-8")
      Command.get(cs) match {
        case Some(cmd) => cmd()
        case None => println(s"$cs is not command")
      }
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

  implicit val akkaSystem = akka.actor.ActorSystem()

  val channel = Seq("channel-1")
  val pattern = Seq("*")

  val actor = akkaSystem.actorOf(Props(classOf[SubscribeActor], channel, pattern))

  akkaSystem.scheduler.scheduleOnce(20 seconds)(Await.result(akkaSystem.terminate(), Duration.Inf))
}