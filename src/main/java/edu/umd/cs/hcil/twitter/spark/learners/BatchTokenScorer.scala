/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.umd.cs.hcil.twitter.spark.learners

import java.io.FileWriter
import java.util.Date

import edu.umd.cs.hcil.twitter.spark.common.{Conf, ScoreGenerator}
import edu.umd.cs.hcil.twitter.spark.utils.{DateUtils, StatusTokenizer}
import org.apache.commons.csv.{CSVFormat, CSVPrinter}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.json4s._
import org.json4s.jackson.JsonMethods._
import twitter4j.{Status, TwitterObjectFactory}

import scala.collection.JavaConverters._
import scala.util.control.Breaks._
import scala.util.parsing.json.{JSONFormat, JSONObject}

object BatchTokenScorer {

  implicit val formats = DefaultFormats // Brings in default date formats etc.
  case class Topic(query: String, num: String, tokens: List[String])

  // Record all tweets we tag
  var taggedTweets : Set[Long] = Set.empty
  var taggedTweetTokens : List[List[String]] = List.empty

  /**
   * @param args the command line arguments
   */
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("TREC Offline Analyzer")
    val sc = new SparkContext(conf)

    val propertiesPath = args(0)
    val dataPath = args(1)
    val topicsFile = args(2)
    val outputFile = args(3)
    val minimumBurstValue = args(4).toDouble

    val burstConf = new Conf(propertiesPath)

    val twitterMsgsRaw = sc.textFile(dataPath)
    println("Initial Partition Count: " + twitterMsgsRaw.partitions.size)

    var twitterMsgs = twitterMsgsRaw
    if (args.size > 5) {
      val initialPartitions = args(5).toInt
      twitterMsgs = twitterMsgsRaw.repartition(initialPartitions)
      println("New Partition Count: " + twitterMsgs.partitions.size)
    }
    val newPartitionSize = twitterMsgs.partitions.size

    val topicsJsonStr = scala.io.Source.fromFile(topicsFile).mkString
    val topicsJson = parse(topicsJsonStr)
    val topicList = topicsJson.extract[List[Topic]]
    val topicKeywordSet: Set[String] = topicList.flatMap(topic => topic.tokens).toSet

    val broad_topicKeywordSet = sc.broadcast(topicKeywordSet)

    // If we are going to use the direct twitter stream, use TwitterUtils. Else, use socket.
    val twitterStream = twitterMsgs.map(line => {
      try {
        TwitterObjectFactory.createStatus(line)
      } catch {
        case e : Exception => null
      }
    })

    // Remove tweets not in English and other filters
    val noRetweetStream = twitterStream
      .filter(status => {
      status != null &&
        status.getLang.compareToIgnoreCase("en") == 0 &&
        !status.getText.toLowerCase.contains("follow") &&
        status.getHashtagEntities.size <= burstConf.maxHashtags &&
        status.getURLEntities.size <= burstConf.maxUrls
    })

    // Only keep tweets that contain a topic token
    val topicalTweetStream = noRetweetStream.filter(status => {

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
    })

    // Create a (time, status) pair from each tweet, replicated MINOR_WINDOW_SIZE times
    val timedTopicalTweetStream = topicalTweetStream.flatMap(status => {
      val actualTime = DateUtils.convertTimeToSlice(status.getCreatedAt)
      val slidTimes = DateUtils.minorWindowDates(actualTime, burstConf.minorWindowSize)
      slidTimes.map(time => (time, status))
    })

    timedTopicalTweetStream.cache()

    // Pull out the times for all the tweets, and construct a list of
    //  dates the cover this data set
    val times = timedTopicalTweetStream.keys

    // Find the min and max dates from the data
    val timeBounds = times.aggregate((new Date(Long.MaxValue), new Date(Long.MinValue)))((u, t) => {
      var min = u._1
      var max = u._2

      if ( t.before(min) ) {
        min = t
      }

      if ( t.after(max) ) {
        max = t
      }

      (min, max)
    },
      (u1, u2) => {
        var min = u1._1
        var max = u1._2

        if ( u2._1.before(min) ) {
          min = u2._1
        }

        if ( u2._2.after(max) ) {
          max = u2._2
        }

        (min, max)
      })
    val minTime = timeBounds._1
    val maxTime = timeBounds._2
    printf("Min Time: " + minTime + "\n")
    printf("Max Time: " + maxTime + "\n")

    // Construct a keyed RDD that maps ALL POSSIBLE Dates between min and max
    //  Date to empty lists
    val fullKeyList = DateUtils.constructDateList(minTime, maxTime)
    println("Date Key List Size: " + fullKeyList.size)

    // Build a slider of the last MAJOR_WINDOW_SIZE minutes
    var rddCount = 0
    var dateList: List[Date] = List.empty
    var tweetRddList: List[RDD[(Status, List[String])]] = List.empty
    var rddList: List[RDD[Tuple2[String, Map[Date, Int]]]] = List.empty
    for ( time <- fullKeyList ) {
      val dateTag = time
      dateList = dateList :+ dateTag

      println("Window Count: " + rddCount)
      println("Dates so far: " + dateList)

      // Need to filter for only those tweets from this time.
      val thisDatesRdd = timedTopicalTweetStream.filter(tuple => {
        val thisTime = tuple._1
        (thisTime.compareTo(time) == 0)
      })

      // Create pairs of statuses and tokens in those statuses
      val tweetTokenPairs = thisDatesRdd.map(tuple => {
        val status = tuple._2
        val tweet = StatusTokenizer.tokenize(status)
        val tokens = tweet ++ status.getHashtagEntities.map(ht => ht.getText)
        (status, tokens.map(str => str.toLowerCase).toList)
      }).filter(tuple => tuple._2.size >= burstConf.minTokens)
      tweetTokenPairs.persist()
      tweetRddList = tweetRddList :+ tweetTokenPairs

      // Convert (tweet, tokens) to (user_id, tokenSet) to (token, 1)
      //  This conversion lets us count only one token per user
      val rdd = tweetTokenPairs
        .map(pair => (pair._1.getUser.getId, pair._2.toSet))
        .reduceByKey(_ ++ _)
        .flatMap(pair => pair._2).map(token => (token, 1))
        .reduceByKey(_ + _)

      // Should be (token, Map[Date, Int])
      val datedPairs = rdd.map(tuple => (tuple._1, Map(dateTag -> tuple._2)))
      println("Date: " + dateTag.toString + ", Token Count: " + datedPairs.count() + ", Tweet Count: " + thisDatesRdd.count())
      datedPairs.persist
      rddList = rddList :+ datedPairs

      val earliestDate = dateList(0)
      println("Earliest Date: " + earliestDate)

      // Merge all the RDDs in our list, so we have a full set of tokens that occur in this window
      val mergingRdd: RDD[Tuple2[String, Map[Date, Int]]] = rddList.reduce((rdd1, rdd2) => {
        rdd1 ++ rdd2
      })

      // Combine all the date maps for each token
      val combinedRddPre: RDD[Tuple2[String, Map[Date, Int]]] = mergingRdd.reduceByKey((a, b) => {
        a ++ b
      })

      val scores: RDD[Tuple2[String, Double]] = ScoreGenerator.scoreFrequencyArray(combinedRddPre, dateList)
      val sortedScores = scores.sortBy(tuple => tuple._2, false)

      val topList = sortedScores.take(20)
      println("\nPopular topics, Now: %s, Window: %s".format(new Date().toString, dateList.last.toString))
      topList.foreach { case (tag, score) => println("%s - %f".format(tag, score)) }

      // Only look for bursty tokens if we're beyond the major window size
      if (rddCount >= burstConf.majorWindowSize) {
        val targetKeywords = sortedScores
          .filter(tuple => tuple._2 > minimumBurstValue)
          .collect
        println("Over threshold count: " + targetKeywords.size)


        // Print JSON string with this date and list of token-score tuples
        // dateTag, targetKeywords
        val datedTokenScoreMap = Map("date" -> dateTag, "pairs" -> targetKeywords.toMap)
        val jsonMap = JSONObject(datedTokenScoreMap)

        // Find the best tweets containing the top tokens and write to output file
        val outputFileWriter = new FileWriter(outputFile, true)
        outputFileWriter.write(jsonMap.toString(jsonFormatter) + "\n")
        outputFileWriter.close()
      }

      // Prune the date and rdd lists as needed
      if (dateList.size == burstConf.majorWindowSize) {

        // Drop the earliest date
        dateList = dateList.slice(1, burstConf.majorWindowSize)

        // Drop the earliest RDD and unpersist it
        val earliestRdd = rddList.head
        rddList = rddList.slice(1, burstConf.majorWindowSize)
        earliestRdd.unpersist(false)

        // Drop the earliest tweet RDD as well
        val earliestTweetRdd = tweetRddList.head
        tweetRddList = tweetRddList.slice(1, burstConf.majorWindowSize)
        earliestTweetRdd.unpersist(false)
      }

      rddCount += 1
    }

  }

  def jsonFormatter (x : Any) : String = {
    x match {
      case m : Map[_, _] => JSONObject(m.asInstanceOf[Map[String,Object]]).toString()
      case d : Date => d.getTime.toString
      case _ => JSONFormat.defaultFormatter(x)
    }
  }
}
