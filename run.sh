#!/bin/sh
spark/bin/spark-submit \
  --class aoi.AreaOfInterestApp \
  --master local[2] \
  --packages com.typesafe:config:1.3.4 \
  ./spark.area.of.interest/target/scala-2.12/spark-area-of-interest_2.12-0.1.jar