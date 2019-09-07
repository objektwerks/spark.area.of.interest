Spark Area of Interest
----------------------
>App that identifies hits within areas of interest.

Source
------
1. AreaOfInterest : csv ( ./data/ ) : (id: String, latitude: Double, longitude: Double)
2. Hit : csv ( ./data/hits/ ) : (id: String, utc: Long, latitude: Double, longitude: Double)

Flow
----
1. Hits **n-days** hence
2. Hits within areas of interest defined by **n-kilometer-radius**

Sink
----
1. HitToAreaOfInterests to log --- console and file ( ./target/app.log )

Run
---
1. sbt clean compile
2. sbt "run 25.0 365" | sbt run ( using built-in default values )
3. Press Ctrl C to stop.

>AreaOfInterestApp takes **2** commandline args.
1. areaOfInterestRadiusInKilometers ( 25.0, in above example, defaults to 25.0 )
2. hitDaysHence ( 365.0 in above example, defaults to 730 )

>**WARNING** The Homebrew install of apache-spark is configured for Scala 2.11, **NOT** Scala 2.12.
>Consequently, the build.sbt is set to Scala 2.11.12. The alternative would be to download and install
>the Scala 2.12 version of Apache Spark via the Apache Spart web site.

Submit
------
>First create a log4j.properties file from log4j.properties.template.
>See: /usr/local/Cellar/apache-spark/2.4.4s/libexec/conf/log4j.properties.template

1. sbt clean compile package
2. chmod +x submit.sh ( required only once )
3. ./submit.sh
4. Press Ctrl C to stop.

>The 2 commandline args are specified in submit.sh.

UI
--
1. SparkUI : localhost:4040
2. History Server UI : localhost:18080 : start-history-server.sh | stop-history-server.shs
 
Log
---
1. ./target/app.log

Events
------
1. /tmp/spark-events