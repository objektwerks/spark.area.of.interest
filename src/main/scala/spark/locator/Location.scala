package spark.locator

import java.time.Instant

case class Location(advertiserId: String, locationAt: Instant, latitude: Double, longitude: Double)

case class AreaOfInterest(name: String, latitude: Double, longitude: Double, radius: Double)