# Real-Time Burst Detection and Summarization in Twitter Streams

This code uses Spark Streaming to identify interesting tokens related to some given set of topics. Tweets containing these tokens are then saved to a file.

Operates by first splitting Twitter stream by topic using Lucene and then identifying bursty tokens in those topic streams.

## Dependencies

This package has a number of dependencies spelled out in `pom.xml` but also expects to be executed via Apache Spark, v2.1.1+.

## Building

To build this code, you need only run `mvn package`, which will produce an "uber" jar with all relevant dependencies.

## Configuration

Primary configuration for this package is set in the `conf/streamer.properties` file, which specifies several configurable options, including window size, match thresholds, the minimum number of tokens in a candidate tweet, etc.

### Configuring Twitter Stream

To connect to a Twitter stream for real-time processing, the developer must also provide a `twitter4j.properties` file (or related configuration for Twitter4j), which specifies OAuth tokens. This properties file is expected to be in the current working directory.

## Executing Stream Processing

To execute this streaming system, you need to submit this code via Spark's `spark-submit` command. An example command is as follows:

    spark-submit \
        --master 'local[*]' \
        --conf spark.ui.port=6886 \
        --executor-memory 2g \
        --conf spark.executor.memoryOverhead=16g \
        --conf spark.driver.maxResultSize=16g \
        --class edu.umd.cs.hcil.twitter.spark.stream.App \
        target/BurstyTwitterStream-2.0-jar-with-dependencies.jar \
        conf/streamer.properties \
        covid19.topic.json \
        cv19.log \
        conf/keywords.txt



