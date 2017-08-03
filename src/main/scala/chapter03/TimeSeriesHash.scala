package chapter03

import akka.util.ByteString
import cats.implicits._
import redis.RedisClient

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

case class TimeSeriesHash(client: RedisClient, namespace: String) {

  import TimeSeriesHash.{FetchResult, Granularity, granularities}

  private def getRoundedTimestamp(timestampInSeconds: Long, precision: Int): Long =
    (Math.floor(timestampInSeconds / precision) * precision).toLong

  private def getKeyName(granularity: Granularity, timestampInSeconds: Long): String = {
    val roundedTimestamp = getRoundedTimestamp(timestampInSeconds, granularity.quantity)
    s"$namespace:${ granularity.name }:${ roundedTimestamp }"
  }

  def insert(timestampInSeconds: Long): Future[Unit] =
    (granularities.map(_._2) map { granularity =>
      val key = getKeyName(granularity, timestampInSeconds)
      val fieldName = getRoundedTimestamp(timestampInSeconds, granularity.duration).toString
      client.hincrby(key, fieldName, 1) flatMap { _ =>
        if (granularity.ttl != -1) client.expire(key, granularity.ttl)
        else Future(true)
      }
    }).toVector.sequence_

  def fetch(granularity: Granularity, beginTimestamp: Long, endTimestamp: Long): Future[Seq[FetchResult]] = {
    val begin = getRoundedTimestamp(beginTimestamp, granularity.duration)
    val end = getRoundedTimestamp(endTimestamp, granularity.duration)
    val keyAndFields = (begin to end by granularity.duration) map { timestamp =>
      (getKeyName(granularity, timestamp), getRoundedTimestamp(timestamp, granularity.duration).toString)
    }

    def getTimestamp(i: Int) = beginTimestamp + i * granularity.duration

    val multi = client.multi()

    val ret = (keyAndFields map { keyAndField =>
      multi.hget[String](keyAndField._1, keyAndField._2)
    }).toVector.sequenceU map { xs =>
      xs.zipWithIndex map { xi =>
        FetchResult(getTimestamp(xi._2), (xi._1 flatMap (x => Try(x.toLong).toOption)).getOrElse(0))
      }
    }
    multi.exec()
    ret
  }
}

object TimeSeriesHash extends App {

  case class Granularity(name: String, ttl: Int, duration: Int, quantity: Int)

  case class FetchResult(timestamp: Long, value: Long)

  val units: Map[Symbol, Int] = Map(
    'second -> 1,
    'minute -> 60,
    'hour -> 60 * 60,
    'day -> 24 * 60 * 60
  )

  val granularities: Map[Symbol, Granularity] = Map(
    'sec -> Granularity("sec", units('hour) * 2, units('second), units('minute) * 5),
    'min -> Granularity("min", units('day) * 7, units('minute), units('hour) * 8),
    'hour -> Granularity("hour", units('day) * 60, units('hour), units('day) * 10),
    'day -> Granularity("day", -1, units('day), units('day) * 30)
  )

  def displayResults(granularityName: String, results: Seq[FetchResult]) = {
    println(s"Results from ${ granularityName }:")
    println("Timestamp \t| Value")
    println("--------------- | ------")
    results map { result =>
      println(s"\t${ result.timestamp }\t| ${ result.value }")
    }
    println()
  }

  implicit val akkaSystem = akka.actor.ActorSystem()

  val client = RedisClient("localhost", 6379)

  val timeSeries = TimeSeriesHash(client, "purchases:item1")
  val beginTimestamp = 0

  val r = for {
    _ <- client.flushall()
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
  //val r = client.flushall()

  Await.result(r, Duration.Inf)
  Await.result(akkaSystem.terminate(), Duration.Inf)
}

