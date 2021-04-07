#!/usr/bin/env bash

spark-submit \
    --master 'local[*]' \
    --conf spark.ui.port=8082 \
    --executor-memory 2g \
    --conf spark.executor.memoryOverhead=16g \
    --conf spark.driver.maxResultSize=16g \
    --class edu.umd.cs.hcil.twitter.spark.stream.App \
    target/BurstyTwitterStream-2.0-jar-with-dependencies.jar \
    ./conf/streamer.properties \
    ./conf/topics.json \
    log_file.log \
    ./conf/filter_keywords.txt
