package chapter03

import redis.RedisClient

import cats._, cats.data._, cats.implicits._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

case class TimeSeriesString(client: RedisClient, namespace: String) {
  import TimeSeriesString.{Granularity, FetchResult, units, granularities}

  private def getRoundedTimestamp(timestampInSeconds: Long, precision: Int): Long =
    (Math.floor(timestampInSeconds / precision) * precision).toLong

  private def getKey(granularity: Granularity, timestampInSeconds: Long): String = {
    val roundedTimestamp = getRoundedTimestamp(timestampInSeconds, granularity.duration)
    s"$namespace:${granularity.name}:${roundedTimestamp}"
  }

  def insert(timestampInSeconds: Long): Future[Unit] =
    (granularities.map(_._2) map { granularity =>
      val key = getKey(granularity, timestampInSeconds)
      client.incr(key) flatMap { _ =>
        if (granularity.ttl != -1) client.expire(key, granularity.ttl)
        else Future(true)
      }
    }).toVector.sequence_

  def fetch(granularity: Granularity, beginTimestamp: Long, endTimestamp: Long): Future[Seq[FetchResult]] = {
    val begin = getRoundedTimestamp(beginTimestamp, granularity.duration)
    val end = getRoundedTimestamp(endTimestamp, granularity.duration)
    val keys = (begin to end by granularity.duration) map (getKey(granularity, _))

    def getTimestamp(i: Int) = beginTimestamp + i * granularity.duration

    client.mget(keys: _*) map { xs =>
      xs.zipWithIndex map {
        case (Some(bs), i) => FetchResult(getTimestamp(i), bs.decodeString("utf-8").toInt)
        case (_, i) => FetchResult(getTimestamp(i), 0)
      }
    }
  }
}

object TimeSeriesString extends App {

  case class Granularity(name: String, ttl: Int, duration: Int)

  case class FetchResult(timestamp: Long, value: Int)

  val units: Map[Symbol, Int] = Map(
    'second -> 1,
    'minute -> 60,
    'hour -> 60 * 60,
    'day -> 24 * 60 * 60
  )

  val granularities: Map[Symbol, Granularity] = Map(
    'sec -> Granularity("sec", units('hour) * 2, units('second)),
    'min -> Granularity("min", units('day) * 7, units('minute)),
    'hour -> Granularity("hour", units('day) * 60, units('hour)),
    'day -> Granularity("day", -1, units('day))
  )

  def displayResults(granularityName: String, results: Seq[FetchResult]) = {
    println(s"Results from ${granularityName}:")
    println("Timestamp \t| Value")
    println("--------------- | ------")
    results map { result =>
      println(s"\t${result.timestamp}\t| ${result.value}")
    }
    println()
  }

  implicit val akkaSystem = akka.actor.ActorSystem()

  val client = RedisClient("localhost", 6379)
  //client.flushall()

  val timeSeries = TimeSeriesString(client, "purchases:item1")
  val beginTimestamp = 0

  val multi = client.multi
  multi.get("purchases:item1:sec:0")
  val r = multi.exec().map { mb =>
    mb.responses match {
      case Some(xs) => xs.map(_.toByteString.decodeString("utf-8"))
      case _ => Nil
    }
  }

  /*
  val r = for {
    _ <- timeSeries.insert(beginTimestamp)
    _ <- timeSeries.insert(beginTimestamp + 1)
    _ <- timeSeries.insert(beginTimestamp + 1)
    _ <- timeSeries.insert(beginTimestamp + 3)
    _ <- timeSeries.insert(beginTimestamp + 61)
    secResult <- timeSeries.fetch(granularities('sec), beginTimestamp, beginTimestamp + 4)
    minResult <- timeSeries.fetch(granularities('min), beginTimestamp, beginTimestamp + 120)
  } yield {
    displayResults(granularities('sec).name, secResult)
    displayResults(granularities('min).name, minResult)
  }
  */

  println(Await.result(r, Duration.Inf))
  Await.result(akkaSystem.terminate(), Duration.Inf)
}
