Spark Locator
-------------
>App that maps locations by date range and meter radius to a set of areas-of-interest.

Source
------
1. Location **parquet** records   (advertiserId: String, locationAt: Long, latitude: Double, longitude: Double)
2. AreaOfInterest **csv** records (name: String, latitude: Double, longitude: Double, radius: Double)

**Parquet formatted Location records will be supported at a future time.**

Flow
----
1. Locations within a **n-day** period
2. Locations within a **n-meter** radius of locations within a set of AreaOfInterests

Sink
----
1. Location-to-AreasOfInterest map to log

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