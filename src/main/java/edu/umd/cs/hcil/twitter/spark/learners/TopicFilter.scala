/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.umd.cs.hcil.twitter.spark.learners

import java.io.FileWriter
import java.util.Date

import edu.umd.cs.hcil.twitter.spark.common.{Conf, ScoreGenerator}
import edu.umd.cs.hcil.twitter.spark.utils.DateUtils
import edu.umd.cs.twitter.tokenizer.TweetTokenizer
import org.apache.commons.csv.{CSVFormat, CSVPrinter}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.json4s._
import org.json4s.jackson.JsonMethods._
import twitter4j.{Status, TwitterObjectFactory}

import scala.collection.JavaConverters._
import scala.util.control.Breaks._

object TopicFilter {

  // Job configuration
  var burstConf : Conf = null

  implicit val formats = DefaultFormats // Brings in default date formats etc.
  case class Topic(title: String, num: String, tokens: List[String])

  // Record all tweets we tag
  var taggedTweets : Set[Long] = Set.empty
  var taggedTweetTokens : List[List[String]] = List.empty

  /**
   * @param args the command line arguments
   */
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("TREC Topic Filter")
    val sc = new SparkContext(conf)

    val dataPath = args(0)
    val topicsFile = args(1)
    val outputFile = args(2)

    val twitterMsgsRaw = sc.textFile(dataPath)
    println("Initial Partition Count: " + twitterMsgsRaw.partitions.size)

    var twitterMsgs = twitterMsgsRaw
    if (args.size > 3) {
      val initialPartitions = args(3).toInt
      twitterMsgs = twitterMsgsRaw.repartition(initialPartitions)
      println("New Partition Count: " + twitterMsgs.partitions.size)
    }
    val newPartitionSize = twitterMsgs.partitions.size

    val topicsJsonStr = scala.io.Source.fromFile(topicsFile).mkString
    val topicsJson = parse(topicsJsonStr)
    val topicList = topicsJson.extract[List[Topic]]
    val topicKeywordSet: Set[String] = topicList.flatMap(topic => topic.tokens).toSet

    val broad_topicKeywordSet = sc.broadcast(topicKeywordSet)

    // read strings into statuses
    val twitterStream = twitterMsgs.map(line => {
      try {
        (line, TwitterObjectFactory.createStatus(line))
      } catch {
        case e : Exception => (line, null)
      }
    })

    // Remove tweets not in English and other filters
    val noRetweetStream = twitterStream
      .filter(tuple => {
      val status = tuple._2

      status != null &&
        status.getLang.compareToIgnoreCase("en") == 0
    })

    // Only keep tweets that contain a topic token
    val topicalTweets = noRetweetStream.filter(tuple => {

      val status = tuple._2
      val localTopicSet = broad_topicKeywordSet.value
      val lowercaseTweet = status.getText.toLowerCase
      val topicIt = localTopicSet.iterator
      var topicalFlag = false

      while (topicIt.hasNext && topicalFlag == false) {
        val topicToken = topicIt.next()

        if (lowercaseTweet.contains(topicToken)) {
          topicalFlag = true
        }
      }

      topicalFlag
    }).map(tuple => tuple._1)

    topicalTweets.saveAsTextFile(outputFile, classOf[org.apache.hadoop.io.compress.GzipCodec])
  }
}
