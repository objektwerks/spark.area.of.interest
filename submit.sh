#!/bin/sh
spark-submit \
  --class aoi.AreaOfInterestApp \
  --master local[2] \
  --packages com.typesafe:config:1.3.4 \
  ./target/scala-2.11/spark-area-of-interest_2.11-0.1.jar \
  30.0 999