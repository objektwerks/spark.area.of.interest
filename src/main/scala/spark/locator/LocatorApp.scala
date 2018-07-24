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
    .option("header", true)
    .schema(locationStructType)
    .csv("./data/locations.txt")
    .as[Location]

  locations
    .filter(location => location.locationAt > 1)
    .map(location => mapLocationToAreaOfInterests(areasOfInterest, location))

  val job = locations
    .writeStream
    .outputMode("complete")
    .format("console")
    .start()
  job.awaitTermination(3000L)
}