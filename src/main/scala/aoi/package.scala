import java.nio.file.{Files, Paths}

import org.apache.log4j.Logger

import scala.util.Try

package object aoi {
  private val logger = Logger.getLogger(getClass.getSimpleName)

  def makeSparkEventLogDir(dir: String): Boolean = {
    val path = Paths.get(dir)
    val created = if (!Files.exists(path)) Try ( Files.createDirectories(path) ).isSuccess else true
    logger.info(s"*** Spark Event Log directory ( $dir ) exists or was created: $created")
    created
  }
}