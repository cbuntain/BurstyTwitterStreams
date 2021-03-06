package edu.umd.cs.hcil.twitter.streamer

import java.util.logging._

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream

import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import twitter4j.Status
import twitter4j.TwitterObjectFactory

import edu.umd.cs.hcil.twitter.spark.common.Conf

object KafkaTwitterStream {

    val LOGGER =
            java.util.logging.Logger.getLogger(KafkaTwitterStream.getClass.getName())

    def getStream(ssc : StreamingContext, conf : Conf) : DStream[Status] = {

        LOGGER.log(Level.INFO, "Spinning up Kafka consumer...")

        val kafkaParams = Map[String, Object](
          "bootstrap.servers" -> conf.kafkaServer,
          "key.deserializer" -> classOf[StringDeserializer],
          "value.deserializer" -> classOf[StringDeserializer],
          "group.id" -> "use_a_separate_group_id_for_each_stream",
          "auto.offset.reset" -> "latest",
          "enable.auto.commit" -> (false: java.lang.Boolean)
        )

        val topics = Array(conf.kafkaTopic)
        val stream = KafkaUtils.createDirectStream[String, String](
          ssc,
          PreferConsistent,
          Subscribe[String, String](topics, kafkaParams)
        )

        val ds_ : DStream[Status] = stream.map(record => TwitterObjectFactory.createStatus(record.value))

        val ds : DStream[Status] = if (conf.debug.equalsIgnoreCase("true")) {
          ds_.map(r => {
            println(r.getId)
            r
          })
        } else {
          ds_
        }

        return ds
    }

}