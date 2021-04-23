#!/usr/bin/env bash





# Use with Spark 2.4.6 in cluster and local
spark-submit \
    --master spark://pscore:7077 \
    --conf spark.ui.port=9999 \
    --executor-memory 2g \
    --conf spark.executor.memoryOverhead=16g \
    --conf spark.driver.maxResultSize=16g \
    --class edu.umd.cs.hcil.twitter.spark.stream.App \
    target/BurstyTwitterStream-2.0-jar-with-dependencies.jar ./conf/streamer.properties ./conf/topics.json log_file.log ./conf/filter_keywords.txt
