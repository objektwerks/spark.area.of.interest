package spark.aoi

import java.time.{Duration, Instant}

import org.apache.spark.sql.SparkSession

import scala.util.Try

object AreaOfInterestApp extends App {
  val daysHence = Try {
    Instant.now.minus(Duration.ofDays(args(0).toInt)).toEpochMilli
  }.toOption.getOrElse(Instant.now.minus(Duration.ofDays(365)).toEpochMilli)

  val radius = Try {
    args(1).toDouble
  }.toOption.getOrElse(25.0)

  val sparkSession = SparkSession.builder.master("local[*]").appName("AreaOfInterest").getOrCreate()
  import sparkSession.implicits._

  sys.addShutdownHook {
    sparkSession.stop
  }

  import AreaOfInterest._
  val areasOfInterest = sparkSession
    .read
    .option("header", true)
    .schema(areaOfInterestStructType)
    .csv("./data/areas_of_interest.txt")
    .as[AreaOfInterest]
    .collect
    .toList
  val areaOfInterestsToHit = mapAreaOfInterestsToHit(areasOfInterest, radius)(_:Hit)

  import Hit._
  val hits = sparkSession
    .readStream
    .option("basePath", "./data/hits")
    .option("header", true)
    .schema(hitStructType)
    .csv("./data/hits")
    .as[Hit]
    .filter(hit => hit.utc > daysHence)
    .map(hit => areaOfInterestsToHit(hit))
    .as[Map[AreaOfInterest, Hit]]
    .writeStream
    .foreach(areaOfInterestsToHitForeachWriter)
    .start()

  hits.awaitTermination
}