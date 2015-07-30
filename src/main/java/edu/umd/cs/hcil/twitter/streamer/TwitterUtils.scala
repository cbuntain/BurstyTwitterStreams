package edu.umd.cs.hcil.twitter.streamer

import twitter4j._
import twitter4j.auth.Authorization
import twitter4j.conf.ConfigurationBuilder
import twitter4j.auth.OAuthAuthorization

import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.Logging
import org.apache.spark.streaming.receiver.Receiver

/**
 * Created by cbuntain on 7/19/15.
 */
object TwitterUtils {
  /**
   * Create a input stream that returns tweets received from Twitter.
   * @param ssc         StreamingContext object
   * @param twitterAuth Twitter4J authentication, or None to use Twitter4J's default OAuth
   *        authorization; this uses the system properties twitter4j.oauth.consumerKey,
   *        twitter4j.oauth.consumerSecret, twitter4j.oauth.accessToken and
   *        twitter4j.oauth.accessTokenSecret
   * @param filters Set of filter strings to get only those tweets that match them
   * @param storageLevel Storage level to use for storing the received objects
   */
  def createStream(
                    ssc: StreamingContext,
                    twitterAuth: Option[Authorization],
                    filters: Seq[String] = Nil,
                    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
                    ): ReceiverInputDStream[Status] = {
    new TwitterInputDStream(ssc, twitterAuth, filters, storageLevel)
  }

}
