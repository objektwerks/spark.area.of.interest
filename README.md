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
2. Hits within **n-kilometer-radius** areas of interest

Sink
----
1. Map[AreasOfInterest, Hit] to log

Run
---
1. sbt clean compile run 15.0 30

>AreaOfInterestApp takes **2** commandline args.
1. areaOfInterestRadiusInKilometers ( 15.0 in above example, defaults to 25.0 )
2. hitDaysHence ( 30 in above example, defaults to 365 )

Web
---
1. http://192.168.1.8:4040

Stop
----
1. Control-C
 
Output
------
1. ./target/app.log