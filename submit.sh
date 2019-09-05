#!/bin/sh
spark-submit \
  --class aoi.AreaOfInterestApp \
  --master local[2] \
  --packages com.typesafe:config:1.3.4 \
  ./target/scala-2.12/spark-area-of-interest_2.12-0.1-SNAPSHOT.jar \
  30.0 999