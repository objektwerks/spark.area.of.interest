package spark.locator

import org.apache.spark.sql.SparkSession

object LocatorApp extends App {
  val sparkSession = SparkSession.builder.master("local[*]").appName("Locator").getOrCreate()
  sys.addShutdownHook(sparkSession.stop)
}