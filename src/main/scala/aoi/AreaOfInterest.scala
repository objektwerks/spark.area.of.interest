package aoi

import java.lang.Math.{atan2, cos, sin, sqrt}
import java.time.{Duration, Instant}

import org.apache.log4j.Logger
import org.apache.spark.sql.Encoders

import scala.collection.mutable
import scala.util.Try

case class AreaOfInterest(id: String, latitude: Double, longitude: Double)

case class Hit(id: String, utc: Long, latitude: Double, longitude: Double)

case class HitToAreaOfInterests(hitId: String, aoiIds: Array[String])

object AreaOfInterest {
  private val logger = Logger.getLogger(this.getClass)
  private val earthRadiusInKilometers = 6371

  val areaOfInterestStructType = Encoders.product[AreaOfInterest].schema
  val hitStructType = Encoders.product[Hit].schema
  val hitToAreaOfInterests = Encoders.product[HitToAreaOfInterests]

  def createSparkEventsDir(dir: String): Boolean = {
    import java.nio.file.{Files, Paths}
    val path = Paths.get(dir)
    if (!Files.exists(path)) Try ( Files.createDirectories(path) ).isSuccess
    else true
  }

  def daysToEpochMillis(days: Long): Long = Instant.now.minus(Duration.ofDays(days)).toEpochMilli

  def mapHitToAreaOfInterests(areaOfInterests: Array[AreaOfInterest],
                              areaOfInterestRadiusInKilometers: Double,
                              hit: Hit): HitToAreaOfInterests = {
    val buffer = new mutable.ArrayBuffer[String]
    areaOfInterests.foreach { areaOfInterest =>
      if (isHitWithinAreaOfInterest(hit, areaOfInterest, areaOfInterestRadiusInKilometers)) buffer += areaOfInterest.id
      ()
    }
    HitToAreaOfInterests(hit.id, buffer.toArray)
  }

  /**
    * Haversine Algo
    */
  private def isHitWithinAreaOfInterest(hit: Hit,
                                        areaOfInterest: AreaOfInterest,
                                        areaOfInterestRadiusInKilometers: Double): Boolean = {
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
    val isHit = if (distanceBetweenHitAndAreaOfInterest < areaOfInterestRadiusInKilometers) true else false
    logger.info("--------------------------------------------------")
    logger.info(s"Hit = $isHit")
    logger.info(s"$hit")
    logger.info(s"$areaOfInterest")
    logger.info(s"Delta: $distanceBetweenHitAndAreaOfInterest")
    logger.info(s"Radius: $areaOfInterestRadiusInKilometers")
    logger.info("--------------------------------------------------")
    isHit
  }
}