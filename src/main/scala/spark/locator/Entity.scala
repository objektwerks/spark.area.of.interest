package spark.locator

import java.time.LocalDateTime

object Entity {
  case class Location(advertiserId: String, locationAt: LocalDateTime, latitude: Double, longitude: Double)

  case class LocationOfInterest(name: String, latitude: Double, longitude: Double, radius: Double)
}