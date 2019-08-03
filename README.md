Spark Area of Interest
----------------------
>App that identifies hits within areas of interest.

Source
------
1. AreaOfInterest : csv : (id: String, latitude: Double, longitude: Double)
2. Hit : csv : (id: String, utc: Long, latitude: Double, longitude: Double)

Flow
----
1. Hits **n-days** hence
2. Hits within areas of interest defined by **n-kilometer-radius**

Sink
----
1. Map[AreasOfInterest, Hit] to log

Run
---
1. sbt clean compile run 15.0 30

>AreaOfInterestApp takes **2** commandline args.
1. areaOfInterestRadiusInKilometers ( 15.0 in above example, defaults to 25.0 )
2. hitDaysHence ( 30 in above example, defaults to 365 )

UI
--
1. SparkUI : localhost:4040
2. History Server UI : localhost:18080 : start-history-server.sh | stop-history-server.sh

Stop
----
1. Control-C
 
Log
---
1. ./target/app.log

Events
------
1. /tmp/spark-events