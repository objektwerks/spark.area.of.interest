Spark Area of Interest
----------------------
>App that identifies hits within an area of interest.

Source
------
1. AreaOfInterest : csv : (id: String, latitude: Double, longitude: Double, radius: Double)
2. Hit : csv : (id: String, utc: Long, latitude: Double, longitude: Double)

Flow
----
1. Hits within an **n-day** period
2. Hits within an **n-kilometer** radius of an area of interest(s)

Sink
----
1. AreasOfInterest * ---> 1 Hit map to log

Run
---
1. sbt clean compile run

Web
---
1. http://192.168.1.8:4040

Stop
----
1. Control-C
 
Output
------
1. ./target/app.log
