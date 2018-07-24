Spark Locator
-------------
>App that maps locations by date range and meter radius to a set of areas-of-interest.

Source
------
1. Location **parquet** records   (advertiserId: String, locationAt: Instant, latitude: Double, longitude: Double)
2. AreaOfInterest **csv** records (name: String, latitude: Double, longitude: Double, radius: Double)

Flow
----
1. Locations within a 30-day period
2. Locations within a 50 meter radius of locations within a set of AreaOfInterests

Sink
----
1. AreaOfInterest -> Locations map

Test
----
1. sbt clean test

Run
---
1. sbt clean test run
 
Output
------
1. console
2. ./target/app.log
3. ./target/test.log