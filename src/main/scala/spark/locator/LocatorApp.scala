package spark.locator

import java.time.{Duration, Instant}

import org.apache.spark.sql.SparkSession

object LocatorApp extends App {
  val sparkSession = SparkSession.builder.master("local[*]").appName("Locator").getOrCreate()
  import sparkSession.implicits._

  sys.addShutdownHook(sparkSession.stop)

  val ThirtyDaysHence = Instant.now.minus(Duration.ofDays(30)).toEpochMilli

  import AreaOfInterest._
  val areasOfInterest = sparkSession
    .read
    .option("header", true)
    .schema(areaOfInterestStructType)
    .csv("./data/areas_of_interest.txt")
    .as[AreaOfInterest]
    .collect
    .toList

  import Location._
  val locations = sparkSession
    .readStream
    .option("header", true)
    .schema(locationStructType)
    .csv("./data/locations.txt")
    .as[Location]

  locations
    .filter(location => location.locationAt > ThirtyDaysHence)
    .map(location => mapLocationToAreaOfInterests(areasOfInterest, location))

  val job = locations
    .writeStream
    .outputMode("complete")
    .format("console")
    .start()
  job.awaitTermination(3000L)
}