package spark

import java.lang.Math.{atan2, cos, sin, sqrt}
import java.time.{Duration, Instant}

import org.apache.spark.sql.ForeachWriter

package object locator {
  private val earthRadiusInMeters = 6371 * 1000
  val ThirtyDaysHence = Instant.now.minus(Duration.ofDays(30)).toEpochMilli
  val mapLocationToAreaOfInterestsForeachWriter = new ForeachWriter[Map[AreaOfInterest, Location]] {
    override def open(partitionId: Long, version: Long): Boolean = true
    override def process(map: Map[AreaOfInterest, Location]): Unit = println(map.toString)
    override def close(errorOrNull: Throwable): Unit = ()
  }

  def mapLocationToAreaOfInterests(areaOfInterests: List[AreaOfInterest],
                                   location: Location): Map[AreaOfInterest, Location] = {
    areaOfInterests
      .flatMap { areaOfInterest =>
        val t = isLocationWithinAreaOfInterest(areaOfInterest, location)
        println(t)
        t
      }.toMap
  }
  /**
    * Haversine Algo
    */
  private def isLocationWithinAreaOfInterest(areaOfInterest: AreaOfInterest,
                                             location: Location): Option[(AreaOfInterest, Location)] = {
    val deltaLatitude = (location.latitude - areaOfInterest.latitude).toRadians
    val deltaLongitude = (location.longitude - areaOfInterest.longitude).toRadians
    val areaOfInterestLatitudeInRadians = areaOfInterest.latitude.toRadians
    val locationLatitudeInRadians = location.latitude.toRadians
    val a = {
      sin(deltaLatitude / 2) *
        sin(deltaLatitude / 2) +
        sin(deltaLongitude / 2) *
          sin(deltaLongitude / 2) *
          cos(areaOfInterestLatitudeInRadians) *
          cos(locationLatitudeInRadians)
    }
    val c = 2 * atan2(sqrt(a), sqrt(1 - a))
    val distanceBetweenAreaOfInterestAndLocation = earthRadiusInMeters * c
    if (distanceBetweenAreaOfInterestAndLocation < areaOfInterest.radius)
      Some((areaOfInterest, location))
    else None
  }
}