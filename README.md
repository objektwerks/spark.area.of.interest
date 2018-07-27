Spark Area of Interest
----------------------
>App that identifies hits within areas of interest.

Source
------
1. AreaOfInterest : csv : (id: String, latitude: Double, longitude: Double, radius: Double)
2. Hit : csv : (id: String, utc: Long, latitude: Double, longitude: Double)

Flow
----
1. Hits **n-days** hence
2. Hits **n-kilometers** within radius of areas of interest

Sink
----
1. Map[AreasOfInterest, Hit] to log

Run
---
1. sbt clean compile run 30 25.0

AreaOfInterestApp takes 2 commandline args.
1. daysHence ( defaults to 365)
2. radius ( defaults to 25.0 )
In the example above, 30 = daysHence, 15.0 = radius

Web
---
1. http://192.168.1.8:4040

Stop
----
1. Control-C
 
Output
------
1. ./target/app.log
