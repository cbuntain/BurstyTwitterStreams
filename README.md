# Real-Time Burst Detection and Summarization in Twitter Streams

This code uses Spark Streaming to identify interesting tokens related to some given set of topics. Tweets containing these tokens are then saved to a file.

Operates by first splitting Twitter stream by topic using Lucene and then identifying bursty tokens in those topic streams.

## Dependencies

This package has a number of dependencies spelled out in `pom.xml` but also expects to be executed via Apache Spark, v2.1.1+.

This code also requires Java 1.8 and will fail with newer versions of Java.

## Building

To build this code, you need only run `mvn package`, which will produce an "uber" jar with all relevant dependencies.

## Configuration

Primary configuration for this package is set in the `conf/streamer.properties` file, which specifies several configurable options, including window size, match thresholds, the minimum number of tokens in a candidate tweet, etc.

### Configuring Topic Streams

This software connects to a filtered Twitter stream, where we search for tweets that include a set of keywords or search terms present in a `filter_keywords.txt` file you provide to `spark-submit`.

Within that Twitter stream, you can also subdivide the stream into separate topics using the `topics.json` configuration file. This file contains a JSON array of dictionaries, where each element contains the following:

    {
        "topid": "cv19",
        "query": "covid19 OR vaccine OR vacine OR vax OR vaxx OR cv19",
        "description": "Possible description of this topic",
        "narrative": "Test of CV19"
    }

The `topid` field is an ID for this topic, used in logging. The `description` and `narrative` fields are informational, and their values are ignored.

The `query` field, however, defines a Lucene query that can be applied to a tweet's body to connect a particular tweet to this particular topic. This query corresponds to the `MATCH_THRESHOLD` field in the properties file, as the higher this threshold, the more a given tweet must match this query to be considered part of this topic. Alternatively, you can set `MATCH_THRESHOLD` to 0.0, and all tweets will be considered relevant to all topics.

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
        <streamer.properties> \
        <topics.json> \
        <log_file.log> \
        <filter_keywords.txt>


## Run in Docker (with docker-compose)

### Build Image 
  update configuration in ./app/conf
  from root of project build image:  `docker build -t burstytwitterstreams:latest ./app`

### Execute Stream Processing from Docker container
   Spin up spark cluster with bursty container `docker-compose up -d && docker-compose logs`  
