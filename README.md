Spark Locator
-------------
>App that matches locations by date range and meter radius against a set of geolocations.

Sourcess
-------
1. Location **parquet** records
2. AreaOfInterest **csv** records

Flow
----
1. Locations within a 30-day period
2. Locations within a 50 meter radius within a set of AreaOfInterests

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