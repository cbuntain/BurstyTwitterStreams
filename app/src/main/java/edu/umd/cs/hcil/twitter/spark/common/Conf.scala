/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.umd.cs.hcil.twitter.spark.common

import java.util.Properties
import java.io.FileInputStream

@SerialVersionUID(1000L)
class Conf(propsFilePath : String) extends Serializable {

  val prop = new Properties()
  prop.load(new FileInputStream(propsFilePath))

  val minorWindowSize : Integer = prop.getProperty("MINOR_WINDOW_SIZE").toInt
  val majorWindowSize : Integer  = prop.getProperty("MAJOR_WINDOW_SIZE").toInt

  val perMinuteMax : Integer  = prop.getProperty("PER_MINUTE_MAX").toInt
  val burstThreshold : Double  = prop.getProperty("BURST_THRESHOLD").toDouble
  val similarityThreshold : Double  = prop.getProperty("SIM_THRESHOLD").toDouble
  val queryThreshold : Double  = prop.getProperty("QUERY_THRESHOLD").toDouble

  val maxHashtags : Integer  = prop.getProperty("MAX_HASHTAGS").toInt
  val minTokens : Integer  = prop.getProperty("MIN_TOKENS").toInt
  val maxUrls : Integer  = prop.getProperty("MAX_URLS").toInt

  val queryExpansionMatchThreshold : Double = prop.getProperty("MATCH_THRESHOLD", "0.2").toDouble
  val queryExpMinTokenSize : Int = prop.getProperty("MIN_TOKEN_LENGTH", "3").toInt
  val queryExpMinTweetCount : Int = prop.getProperty("MIN_TWEET_COUNT", "100").toInt
  val queryExpMinHashtagCount : Int = prop.getProperty("MIN_HASHTAG_COUNT", "20").toInt

  val maxRelevantTweets : Int = prop.getProperty("MAX_RELEVANT_TWEETS", "1000").toInt
  val queryExpTokenTakeCount : Int = prop.getProperty("MAX_TAKEN_TOKENS", "10").toInt
  val queryExpHashtagTakeCount : Int = prop.getProperty("MAX_TAKEN_TAGS", "3").toInt

  val makeBigrams : Boolean = prop.getProperty("USE_BIGRAMS", "False").toBoolean

  val brokerUrl : String = prop.getProperty("BROKER_URL", "localhost:8080")
  val clientId : String = prop.getProperty("CLIENT_ID", "tC6WXoIlnW8a")

  val thresholdModifier : Double = 0.75

  val retweetThreshold : Int = prop.getProperty("RETWEET_THRESHOLD", "100").toInt

  val kafkaServer : String = prop.getProperty("KAFKA_SERVER", "localhost:9092")
  val kafkaTopic : String = prop.getProperty("KAFKA_TOPIC", "burst")

  val useKafka : String = prop.getProperty("USE_KAFKA", "false")
  val useReplay : String = prop.getProperty("USE_REPLAY", "false")

  val debug : String = prop.getProperty("DEBUG", "false")

//
//  def MINOR_WINDOW_SIZE = 2
//  def MAJOR_WINDOW_SIZE = 30 // Used 30 minutes here for TREC2015
//
//  def PER_MINUTE_MAX = 10
//  def BURST_THRESHOLD = 0.07
//  def SIM_THRESHOLD = 0.7
//
//  def MAX_HASHTAGS = 3
//  def MAX_URLS = 2
//  def MIN_TOKENS = 5
//


}
