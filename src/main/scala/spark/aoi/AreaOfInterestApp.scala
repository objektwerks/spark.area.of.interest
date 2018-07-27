package spark.aoi

import org.apache.spark.sql.SparkSession

object AreaOfInterestApp extends App {
  val sparkSession = SparkSession.builder.master("local[*]").appName("Locator").getOrCreate()
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
  val areaOfInterestsToHit = mapAreaOfInterestsToHit(areasOfInterest)(_:Hit)

  import Hit._
  val hits = sparkSession
    .readStream
    .option("basePath", "./data/hits")
    .option("header", true)
    .schema(hitStructType)
    .csv("./data/hits")
    .as[Hit]
    .filter(hit => hit.utc > ThirtyDaysHence)
    .map(hit => areaOfInterestsToHit(hit))
    .as[Map[AreaOfInterest, Hit]]
    .writeStream
    .foreach(areaOfInterestsToHitForeachWriter)
    .start()

  hits.awaitTermination
}