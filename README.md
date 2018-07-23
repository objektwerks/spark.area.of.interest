Spark Locator
-------------
>The purpose of the project is to locate targets by date range and within meter radius across a set of geolocations.

Sources
-------
1. Location **parquet** records.
2. AreaOfInterest **csv** records.

Flow
----
1. select Locations for a 30-day period
2. where a Location is within a 50 meter radius of a AreaOfInterest in a set of AreaOfInterests

Sink
----
1. Location log.

Considerations
--------------

Test
----
1. sbt clean test

Run
---
1. sbt clean test run
 
Output
------
1. console
2. ./target/location.log
3. ./target/app.log
4. ./target/test.logs