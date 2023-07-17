name := "spark.area.of.interest"
organization := "objektwerks"
version := "0.1-SNAPSHOT"
scalaVersion := "2.13.11"
libraryDependencies ++= {
  val sparkVersion = "3.4.1"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-streaming" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "com.typesafe" % "config" % "1.4.2"
  )
}
