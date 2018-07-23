Spark Locator
-------------
>The purpose of the project is to locate targets by date range and meter radius across a set of geolocations.

Sources
-------
1. Location **parquet** records.
2. LocationOfInterest **csv** records.

Search Criteria
---------------
1. Location records within 30-day period
2. Location records within 50 meter radius of LocationOfInterest records.

Results
-------
1. Console
2. Log

Test
----
1. sbt clean test

Run
---
1. sbt clean test run
 
Output
------
1. ./target/app.log
2. ./target/test.log