package spark.aoi

import org.apache.spark.sql.Encoders

case class AreaOfInterest(id: String, latitude: Double, longitude: Double, radius: Double)

object AreaOfInterest {
  val areaOfInterestStructType = Encoders.product[AreaOfInterest].schema
}

case class Hit(id: String, utc: Long, latitude: Double, longitude: Double)

object Hit {
  val hitStructType = Encoders.product[Hit].schema
}