name := "spark.locator"
organization := "objektwerks"
version := "0.1"
scalaVersion := "2.11.12"
libraryDependencies ++= {
  val sparkVersion = "2.3.1"
  Seq(
    "org.apache.spark" % "spark-core_2.11" % sparkVersion,
    "org.apache.spark" % "spark-streaming_2.11" % sparkVersion,
    "org.apache.spark" % "spark-sql_2.11" % sparkVersion,
    "org.slf4j" % "slf4j-api" % "1.7.25",
    "org.scalatest" % "scalatest_2.11" % "3.0.5" % "test"
  )
}
scalacOptions ++= Seq(
  "-language:postfixOps",
  "-language:reflectiveCalls",
  "-language:implicitConversions",
  "-language:higherKinds",
  "-feature",
  "-Ywarn-unused-import",
  "-Ywarn-unused",
  "-Ywarn-dead-code",
  "-unchecked",
  "-deprecation",
  "-Xfatal-warnings",
  "-Xlint:missing-interpolator",
  "-Xlint"
)