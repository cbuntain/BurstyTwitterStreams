# Kafka Producer for Burst Analysis

This code drives a Kafka producer to which the Spark-based stream analysis framework can attach for ingesting Twitter content in real time. This code additionally allows you to have multiple Spark executors ingest the stream simultaneously *and* store the content streamed for later analysis.

This code is also developed to support conjunctive filtering, so you can specify usernames to follow, a GPS bounding box to filter, *and* a set of keywords that must be present in an incoming tweet before it is passed on to the Spark burst analysis platform.

## Kafka Startup Code

The following commands spin up the Kafka server for ingestion. Code was built using `kafka_2.13-2.7.0.tgz`. 

### Installation Setup

Install Python’s Tweepy package and Kafka package

    pip3 install --user tweepy
    pip3 install --user kafka

### Settings

To consume messages on other machines we need to change the following in kafka folder:

In `bin/config/server.properties`:

    listeners=PLAINTEXT://<host ip address>:9092
    advertised.listeners=PLAINTEXT://<host ip address>:9092
    Log.retention.hours: 24

In ProducerKafka.py:

    producer = KafkaProducer(bootstrap_servers='<host ip address>:9092')

### Steps to run Kafka

After setting the values in `server.properties` and `ProducerKafka.py` we can get started with Kafka

__Step 1:__ Start the Kafka Server and Zookeeper. To get the kafka running for Twitter collection, first we need to set up zookeeper server, which works as a coordinator to maintain the nodes in Kafka. To run zookeeper, we need to run the below command in the kafka folder

    bin/zookeeper-server-start.sh ./config/zookeeper.properties


Next is to Start the Kafka broker service

    bin/kafka-server-start.sh ./config/server.properties


__Step 2:__ Create a topic 

    ./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic <Topic-Name>


__Step 3:__ Run the Producer 

After Starting the zookeeper, kafka broker service and creating a topic name, we can run our program. The program contains the producer.

    python3 ProducerKafka.py COVID19_Keywords_1.txt 

__Step 4:__ After running the producer, we *can* run a console consumer to capture all the tweets produced and ensure this pipeline is working. This command only pipes content out to your console for debugging.

    bin/kafka-console-consumer.sh --bootstrap-server <host ip address>:9092 --topic <topic_name> --from-beginning


This captures the tweets from the beginning. If we do not want the messages from beginning we can remove that option.

After all the steps have been completed, we can receive the messages from any machine using the consumer command given above as shown below.
