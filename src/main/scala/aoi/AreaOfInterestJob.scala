package aoi

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.Try

object AreaOfInterestJob {
  import AreaOfInterest._

  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger(getClass.getSimpleName)
    val conf = ConfigFactory.load("job.conf").getConfig("job")

    val areaOfInterestRadiusInKilometers = Try(args(0).toDouble).getOrElse(25.0)
    val hitDaysHence = Try(daysToEpochMillis(args(1).toLong)).getOrElse(daysToEpochMillis(730))
    logger.info(s"*** areaOfInterestRadiusInKilometers: $areaOfInterestRadiusInKilometers")
    logger.info(s"*** hitDaysHence: $hitDaysHence")

    makeSparkEventLogDir(conf.getString("spark.eventLog.dir"))

    runJob(logger, conf, areaOfInterestRadiusInKilometers, hitDaysHence)
  }

  def runJob(logger: Logger, conf: Config, areaOfInterestRadiusInKilometers: Double, hitDaysHence: Long): Unit = {
    val sparkConf = new SparkConf()
      .setMaster(conf.getString("master"))
      .setAppName(conf.getString("name"))
      .set("spark.eventLog.enabled", conf.getBoolean("spark.eventLog.enabled").toString)
      .set("spark.eventLog.dir", conf.getString("spark.eventLog.dir"))
      .set("spark.serializer", conf.getString("spark.serializer"))
      .set("spark.kryo.registrationRequired", "true")
      .registerKryoClasses( Array(
        classOf[AreaOfInterest],
        classOf[Array[AreaOfInterest]],
        classOf[Hit],
        classOf[Array[Hit]],
        classOf[HitToAreaOfInterests],
        classOf[Array[HitToAreaOfInterests]]
      ))

    val sparkSession = SparkSession
      .builder
      .config(sparkConf)
      .getOrCreate()
    logger.info("*** AreaOfInterestApp Spark session built. Press Ctrl C to stop.")

    sys.addShutdownHook {
      sparkSession.stop
      logger.info("*** AreaOfInterestApp Spark session stopped.")
    }

    import sparkSession.implicits._

    val broadcastAreasOfInterest = sparkSession.sparkContext.broadcast(
      sparkSession
        .read
        .format("csv")
        .option("header", true)
        .option("delimiter", ",")
        .schema(areaOfInterestStructType)
        .load(conf.getString("aoi"))
        .as[AreaOfInterest]
        .collect
    )

    val hits = sparkSession
      .readStream
      .option("basePath", conf.getString("hits"))
      .option("header", true)
      .option("delimiter", ",")
      .schema(hitStructType)
      .csv(conf.getString("hits"))
      .as[Hit]
      .filter(hit => hit.utc > hitDaysHence)
      .map(hit => mapHitToAreaOfInterests(broadcastAreasOfInterest.value, areaOfInterestRadiusInKilometers, hit))
      .as[HitToAreaOfInterests]
      .writeStream
      .format("console")
      .option("truncate", "false")
      .start
    hits.awaitTermination
    ()
  }
}