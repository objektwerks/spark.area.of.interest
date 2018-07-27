package spark

import java.lang.Math.{atan2, cos, sin, sqrt}

import org.apache.log4j.Logger
import org.apache.spark.sql.ForeachWriter

package object aoi {
  private val logger = Logger.getLogger(this.getClass)
  private val earthRadiusInKilometers = 6371

  val areaOfInterestsToHitForeachWriter = new ForeachWriter[Map[AreaOfInterest, Hit]] {
    override def open(partitionId: Long, version: Long): Boolean = true
    override def process(areasOfInterest: Map[AreaOfInterest, Hit]): Unit = {
      logger.info("**************************************************")
      areasOfInterest.foreach { case (areaOfInterest, hit) =>
        logger.info(s"$areaOfInterest")
        logger.info(s"\t$hit")
      }
      logger.info("**************************************************")
    }
    override def close(errorOrNull: Throwable): Unit = ()
  }

  def mapAreaOfInterestsToHit(areaOfInterests: List[AreaOfInterest], areaOfInterestRadius: Double)
                             (hit: Hit): Map[AreaOfInterest, Hit] = {
    areaOfInterests.flatMap { areaOfInterest =>
      isHitWithinAreaOfInterest(hit, areaOfInterest, areaOfInterestRadius).map(hit => areaOfInterest -> hit)
    }.toMap
  }

  /**
    * Haversine Algo
    */
  private def isHitWithinAreaOfInterest(hit: Hit,
                                        areaOfInterest: AreaOfInterest,
                                        areaOfInterestRadius: Double): Option[Hit] = {
    val deltaLatitude = (hit.latitude - areaOfInterest.latitude).toRadians
    val deltaLongitude = (hit.longitude - areaOfInterest.longitude).toRadians
    val areaOfInterestLatitudeInRadians = areaOfInterest.latitude.toRadians
    val locationLatitudeInRadians = hit.latitude.toRadians
    val a = {
      sin(deltaLatitude / 2) *
        sin(deltaLatitude / 2) +
        sin(deltaLongitude / 2) *
          sin(deltaLongitude / 2) *
          cos(areaOfInterestLatitudeInRadians) *
          cos(locationLatitudeInRadians)
    }
    val c = 2 * atan2(sqrt(a), sqrt(1 - a))
    val distanceBetweenHitAndAreaOfInterest = earthRadiusInKilometers * c
    logger.info("**************************************************")
    logger.info(s"$hit")
    logger.info(s"$areaOfInterest")
    logger.info(s"delta distance: $distanceBetweenHitAndAreaOfInterest radius: $areaOfInterestRadius")
    logger.info("**************************************************")
    if (distanceBetweenHitAndAreaOfInterest < areaOfInterestRadius) Some(hit) else None
  }
}