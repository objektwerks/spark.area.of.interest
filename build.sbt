name := "spark.area.of.interest"
organization := "objektwerks"
version := "0.1-SNAPSHOT"
scalaVersion := "2.12.12"
libraryDependencies ++= {
  val sparkVersion = "2.4.7"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-streaming" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "com.typesafe" % "config" % "1.4.0"
  )
}
