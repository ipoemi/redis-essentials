package chapter04

import com.github.nscala_time.time.Imports.DateTime
import redis._
import redis.api.pubsub.Message

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object Subscriber extends App {

  val Command = Map(
    "DATE" -> (() => println(s"Date ${DateTime.now}")),
    "PING" -> (() => println("PONG")),
    "HOSTNAME" -> (() => println(s"HOSTNAME ${java.net.InetAddress.getLocalHost.getHostName}"))
  )

  implicit val akkaSystem = akka.actor.ActorSystem()

  val channel = Seq("channel-1")

  val client = RedisPubSub("localhost", 6379, channel, Seq("*"), {
    case Message(ch, bs) => {
      val cs = bs.decodeString("utf-8")
      Command.get(cs) match {
        case Some(cmd) => cmd()
        case None => println(s"$cs is not command")
      }
    }
  })

  akkaSystem.scheduler.scheduleOnce(20 seconds)(Await.result(akkaSystem.terminate(), Duration.Inf))
}
