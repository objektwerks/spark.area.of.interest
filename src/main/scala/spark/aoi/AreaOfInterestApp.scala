package spark.aoi

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

import scala.util.Try

object AreaOfInterestApp extends App {
  val conf = ConfigFactory.load("app.conf").getConfig("app")
  val areaOfInterestRadiusInKilometers = Try(args(0).toDouble).getOrElse(25.0)
  val hitDaysHence = Try(daysToEpochMillis(args(1).toLong)).getOrElse(daysToEpochMillis(365))

  val sparkSession = SparkSession
    .builder
    .master(conf.getString("master"))
    .appName(conf.getString("name"))
    .getOrCreate()
  import sparkSession.implicits._

  sys.addShutdownHook {
    sparkSession.stop
  }

  val areasOfInterest = sparkSession
    .read
    .option("header", true)
    .schema(areaOfInterestStructType)
    .csv(conf.getString("csv"))
    .as[AreaOfInterest]
    .collect
    .toList
  val areaOfInterestsToHit = mapAreaOfInterestsToHit(areasOfInterest, areaOfInterestRadiusInKilometers)(_:Hit)

  val hits = sparkSession
    .readStream
    .option("basePath", conf.getString("hits"))
    .option("header", true)
    .schema(hitStructType)
    .csv(conf.getString("hits"))
    .as[Hit]
    .filter(hit => hit.utc > hitDaysHence)
    .map(hit => areaOfInterestsToHit(hit))
    .as[Map[AreaOfInterest, Hit]]
    .writeStream
    .foreach(areaOfInterestsToHitForeachWriter)
    .start()

  hits.awaitTermination
}