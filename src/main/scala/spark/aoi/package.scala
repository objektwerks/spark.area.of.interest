package spark

import java.lang.Math.{atan2, cos, sin, sqrt}
import java.time.{Duration, Instant}

import org.apache.log4j.Logger
import org.apache.spark.sql.{Encoders, ForeachWriter}

package object aoi {
  private val logger = Logger.getLogger(this.getClass)
  private val earthRadiusInKilometers = 6371

  val areaOfInterestStructType = Encoders.product[AreaOfInterest].schema
  val hitStructType = Encoders.product[Hit].schema

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

  def daysToEpochMillis(days: Long): Long = Instant.now.minus(Duration.ofDays(days)).toEpochMilli

  def mapAreaOfInterestsToHit(areaOfInterests: List[AreaOfInterest], areaOfInterestRadiusInKilometers: Double)
                             (hit: Hit): Map[AreaOfInterest, Hit] = {
    areaOfInterests.flatMap { areaOfInterest =>
      isHitWithinAreaOfInterest(hit, areaOfInterest, areaOfInterestRadiusInKilometers).map(hit => areaOfInterest -> hit)
    }.toMap
  }

  /**
    * Haversine Algo
    */
  private def isHitWithinAreaOfInterest(hit: Hit,
                                        areaOfInterest: AreaOfInterest,
                                        areaOfInterestRadiusInKilometers: Double): Option[Hit] = {
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
    logger.info(s"delta distance: $distanceBetweenHitAndAreaOfInterest radius: $areaOfInterestRadiusInKilometers")
    logger.info("**************************************************")
    if (distanceBetweenHitAndAreaOfInterest < areaOfInterestRadiusInKilometers) Some(hit) else None
  }
}