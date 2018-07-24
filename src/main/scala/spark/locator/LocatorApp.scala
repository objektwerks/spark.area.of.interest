package spark.locator

import org.apache.spark.sql.SparkSession

object LocatorApp extends App {
  val sparkSession = SparkSession.builder.master("local[*]").appName("Locator").getOrCreate()
  import sparkSession.implicits._

  sys.addShutdownHook(sparkSession.stop)

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
    .option("basePath", "./data/location")
    .option("header", true)
    .schema(locationStructType)
    .csv("./data/location")
    .as[Location]

  val job = locations
    .filter(location => location.locationAt > ThirtyDaysHence)
    .map(location => mapLocationToAreaOfInterests(areasOfInterest, location))
    .as[Map[AreaOfInterest, Location]]
    .writeStream
    .foreach(mapLocationToAreaOfInterestsForeachWriter)
    .start()

  job.awaitTermination(3000L)
}