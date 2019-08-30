package aoi

import java.nio.file.{Files, Paths}

import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.Try

object AreaOfInterestApp {
  private val logger = Logger.getLogger(this.getClass)

  private def makeEventsDir(dir: String): Boolean = {
    val path = Paths.get(dir)
    if (!Files.exists(path)) Try ( Files.createDirectories(path) ).isSuccess else true
  }

  def main(args: Array[String]): Unit = {
    import AreaOfInterest._

    logger.info(s"*** Main args(0): ${args(0)} : args(1): ${args(1)}")
    val areaOfInterestRadiusInKilometers = Try(args(0).toDouble).getOrElse(25.0)
    val hitDaysHence = Try(daysToEpochMillis(args(1).toLong)).getOrElse(daysToEpochMillis(365))
    logger.info(s"*** areaOfInterestRadiusInKilometers: $areaOfInterestRadiusInKilometers")
    logger.info(s"*** hitDaysHence: $hitDaysHence")

    val conf = ConfigFactory.load("app.conf").getConfig("app")

    val sparkEventLogDir = conf.getString("spark.eventLog.dir")
    val sparkEventDirCreated = makeEventsDir(sparkEventLogDir)
    logger.info(s"*** $sparkEventLogDir exists or was created: $sparkEventDirCreated")

    val sparkConf = new SparkConf()
      .setMaster(conf.getString("master"))
      .setAppName(conf.getString("name"))
      .set("spark.serializer", conf.getString("spark.serializer"))
      .set("spark.eventLog.enabled", conf.getBoolean("spark.eventLog.enabled").toString)
      .set("spark.eventLog.dir", sparkEventLogDir)
      .registerKryoClasses(Array(classOf[AreaOfInterest], classOf[Hit]))

    val sparkSession = SparkSession
      .builder
      .config(sparkConf)
      .getOrCreate()
    logger.info("*** Initialized Spark AreaOfInterestApp. Press Ctrl C to terminate.")

    sys.addShutdownHook {
      sparkSession.stop
      logger.info("*** Terminated Spark AreaOfInterestApp.")
    }

    import sparkSession.implicits._

    val areasOfInterest = sparkSession
      .read
      .format("csv")
      .option("header", true)
      .option("delimiter", ",")
      .schema(areaOfInterestStructType)
      .load(conf.getString("aoi"))
      .as[AreaOfInterest]
      .collect

    val hits = sparkSession
      .readStream
      .option("basePath", conf.getString("hits"))
      .option("header", true)
      .option("delimiter", ",")
      .schema(hitStructType)
      .csv(conf.getString("hits"))
      .as[Hit]
      .filter(hit => hit.utc > hitDaysHence)
      .map(hit => mapHitToAreaOfInterests(areasOfInterest, areaOfInterestRadiusInKilometers, hit))
      .as[HitToAreaOfInterests]
      .writeStream
      .format("console")
      .option("truncate", "false")
      .start
    hits.awaitTermination
    ()
  }
}