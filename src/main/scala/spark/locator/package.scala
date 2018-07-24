package spark

package object locator {
  import java.lang.Math.{atan2, cos, sin, sqrt}
  val earthRadiusInMeters = 6371 * 1000

  /**
   * Haversine Algo
   */
  def distanceInMeters(lat1: Double, lon1: Double)(lat2: Double, lon2: Double): Double = {
    val deltaLat = (lat2 - lat1).toRadians
    val deltaLon = (lon2 - lon1).toRadians
    val latRad1 = lat1.toRadians
    val latRad2 = lat2.toRadians
    val a = sin(deltaLat / 2) * sin(deltaLat / 2) + sin(deltaLon / 2) * sin(deltaLon / 2) * cos(latRad1) * cos(latRad2)
    val c = 2 * atan2(sqrt(a), sqrt(1 - a))
    earthRadiusInMeters * c
  }
}