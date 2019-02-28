package spark.aoi

import org.apache.spark.sql.SparkSession

import scala.util.Try

object AreaOfInterestApp extends App {
  val areaOfInterestRadiusInKilometers = Try(args(0).toDouble).getOrElse(25.0)
  val hitDaysHence = Try(daysToEpochMillis(args(1).toLong)).getOrElse(daysToEpochMillis(365))

  val sparkSession = SparkSession.builder.master("local[*]").appName("AreaOfInterest").getOrCreate()
  import sparkSession.implicits._

  sys.addShutdownHook {
    sparkSession.stop
  }

  val areasOfInterest = sparkSession
    .read
    .option("header", true)
    .schema(areaOfInterestStructType)
    .csv("./data/areas_of_interest.txt")
    .as[AreaOfInterest]
    .collect
    .toList
  val areaOfInterestsToHit = mapAreaOfInterestsToHit(areasOfInterest, areaOfInterestRadiusInKilometers)(_:Hit)

  val hits = sparkSession
    .readStream
    .option("basePath", "./data/hits")
    .option("header", true)
    .schema(hitStructType)
    .csv("./data/hits")
    .as[Hit]
    .filter(hit => hit.utc > hitDaysHence)
    .map(hit => areaOfInterestsToHit(hit))
    .as[Map[AreaOfInterest, Hit]]
    .writeStream
    .foreach(areaOfInterestsToHitForeachWriter)
    .start()

  hits.awaitTermination
}