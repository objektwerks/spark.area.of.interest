package spark

import java.lang.Math.{atan2, cos, sin, sqrt}

package object locator {
  private val earthRadiusInMeters = 6371 * 1000

  def mapLocationToAreaOfInterests(areaOfInterests: List[AreaOfInterest],
                                   location: Location): Map[AreaOfInterest, Location] = {
    areaOfInterests
      .flatMap { areaOfInterest =>
        isLocationWithinAreaOfInterest(areaOfInterest, location)
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