package spark

import java.lang.Math.{atan2, cos, sin, sqrt}
import java.time.{Duration, Instant}

import org.apache.log4j.Logger
import org.apache.spark.sql.ForeachWriter

package object locator {
  private val logger = Logger.getLogger(this.getClass)
  private val earthRadiusInMeters = 6371 * 1000

  val ThirtyDaysHence = Instant.now.minus(Duration.ofDays(30)).toEpochMilli
  val mapLocationToAreaOfInterestsForeachWriter = new ForeachWriter[Map[Location, List[AreaOfInterest]]] {
    override def open(partitionId: Long, version: Long): Boolean = true
    override def process(locations: Map[Location, List[AreaOfInterest]]): Unit = {
      logger.info("**************************************************")
      locations.foreach { case (location, areaOfInterests) =>
        logger.info(s"$location")
        areaOfInterests.foreach { areaOfInterest =>
          logger.info(s"\t$areaOfInterest")
        }
      }
      logger.info("**************************************************")
    }
    override def close(errorOrNull: Throwable): Unit = ()
  }

  def mapLocationToAreaOfInterests(areaOfInterests: List[AreaOfInterest])
                                  (location: Location): Map[Location, List[AreaOfInterest]] = {
    Map(location -> areaOfInterests
      .flatMap { areaOfInterest =>
        isLocationWithinAreaOfInterest(location, areaOfInterest)
      })
  }
  /**
    * Haversine Algo
    */
  private def isLocationWithinAreaOfInterest(location: Location,
                                             areaOfInterest: AreaOfInterest): Option[AreaOfInterest] = {
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
    val distanceBetweenLocationAndAreaOfInterest = earthRadiusInMeters * c
    logger.info("**************************************************")
    logger.info(s"$location")
    logger.info(s"$areaOfInterest")
    logger.info(s"delta distance: $distanceBetweenLocationAndAreaOfInterest radius: ${areaOfInterest.radius}")
    logger.info("**************************************************")
    if (distanceBetweenLocationAndAreaOfInterest < areaOfInterest.radius)
      Some(areaOfInterest)
    else None
  }
}