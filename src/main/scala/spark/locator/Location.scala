package spark.locator

import java.time.Instant

import org.apache.spark.sql.Encoders

case class Location(advertiserId: String, locationAt: Instant, latitude: Double, longitude: Double)

object Location {
  val locationStructType = Encoders.product[Location].schema
}

case class AreaOfInterest(name: String, latitude: Double, longitude: Double, radius: Double)

object AreaOfInterest {
  val areaOfInterestStructType = Encoders.product[AreaOfInterest].schema
}