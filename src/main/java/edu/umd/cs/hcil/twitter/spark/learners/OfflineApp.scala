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
import twitter4j.{Status, TwitterObjectFactory, TwitterException}

import scala.collection.JavaConverters._
import scala.util.control.Breaks._

object OfflineApp {
  val MINOR_WINDOW_SIZE = Conf.MINOR_WINDOW_SIZE
  val MAJOR_WINDOW_SIZE = Conf.MAJOR_WINDOW_SIZE
  val PER_MINUTE_MAX = Conf.PER_MINUTE_MAX
  val THRESHOLD = Conf.BURST_THRESHOLD
  val SIM_THRESHOLD = Conf.SIM_THRESHOLD

  val MAX_HASHTAGS = Conf.MAX_HASHTAGS
  val MAX_URLS = Conf.MAX_URLS
  val MIN_TOKENS = Conf.MIN_TOKENS

  implicit val formats = DefaultFormats // Brings in default date formats etc.
  case class Topic(title: String, num: String, tokens: List[String])

  // Record all tweets we tag
  var taggedTweets : Set[Long] = Set.empty
  var taggedTweetTokens : List[List[String]] = List.empty

  /**
   * @param args the command line arguments
   */
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("TREC Offline Analyzer")
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
        status.getHashtagEntities.size <= MAX_HASHTAGS &&
        status.getURLEntities.size <= MAX_URLS
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
      val slidTimes = DateUtils.minorWindowDates(actualTime, MINOR_WINDOW_SIZE)
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
        val tokenizer = new TweetTokenizer
        val tweet = tokenizer.tokenizeTweet(status.getText)
        val tokens = tweet.getTokens.asScala ++ status.getHashtagEntities.map(ht => ht.getText)
        (status, tokens.map(str => str.toLowerCase).toList)
      }).filter(tuple => tuple._2.size >= MIN_TOKENS)
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

      // Bursty keywords to look for in tweets
      var burstingKeywords : List[String] = List.empty

      // Only look for bursty tokens if we're beyond the major window size
      if (rddCount >= MAJOR_WINDOW_SIZE) {
        val targetKeywords = sortedScores
          .filter(tuple => tuple._2 > THRESHOLD)
          .map(tuple => tuple._1).collect

        println("Over threshold count: " + targetKeywords.size)
        val topTokens: List[String] = targetKeywords.take(10).toList

        burstingKeywords = burstingKeywords ++ topTokens
        println("Bursting Keywords count: " + burstingKeywords.size)
      }

      // Find the best tweets containing the top tokens and write to output file
      val outputFileWriter = new FileWriter(outputFile, true)
      val logEntries = findGoodTweets(time, burstingKeywords, tweetRddList, topicList)
      logEntries.foreach(logEntry => outputFileWriter.write(logEntry))
      outputFileWriter.close()

      // Prune the date and rdd lists as needed
      if (dateList.size == MAJOR_WINDOW_SIZE) {

        // Drop the earliest date
        dateList = dateList.slice(1, MAJOR_WINDOW_SIZE)

        // Drop the earliest RDD and unpersist it
        val earliestRdd = rddList.head
        rddList = rddList.slice(1, MAJOR_WINDOW_SIZE)
        earliestRdd.unpersist(false)

        // Drop the earliest tweet RDD as well
        val earliestTweetRdd = tweetRddList.head
        tweetRddList = tweetRddList.slice(1, MAJOR_WINDOW_SIZE)
        earliestTweetRdd.unpersist(false)
      }

      rddCount += 1
    }

  }

  def createCsvString(topic : String, time : Date, tweetId : Long, text : String) : String = {
    val buff = new StringBuffer()
    val writer = new CSVPrinter(buff, CSVFormat.DEFAULT)

    writer.print(topic)
    writer.print(time.getTime / 1000)
    writer.print(tweetId)
    writer.print(text.replace("\n", " "))

    buff.toString + "\n"
  }

  def findGoodTweets(
                      time : Date,
                      targetTokens : List[String],
                      tweetRddList: List[RDD[(Status, List[String])]],
                      topicList : List[OfflineApp.Topic]) : List[String] = {

    println("Status RDD Time: " + time)

    var logEntries : List[String] = List.empty
    var capturedTweets: Map[Status, Int] = Map.empty

    println("Bursting Keyword Count: " + targetTokens.size)
    println("Finding tweets containing: %s".format(targetTokens))

    val rdd = tweetRddList.reduce((l, r) => l ++ r)
    val targetTweets = rdd.filter(tuple => {
      val status = tuple._1
      var flag = false

      for (token <- targetTokens) {
        if (status.getText.toLowerCase.contains(token)) {
          flag = true
        }
      }
      flag
    }).collect().toMap

    for (tweet <- targetTweets.keys) {
      capturedTweets = capturedTweets ++ Map(tweet -> (capturedTweets.getOrElse(tweet, 0) + 1))
    }

    val topMatches: List[Status] = capturedTweets
      .filter(tuple => tuple._2 == capturedTweets.values.max)
      .map(tuple => tuple._1)
      .toList
      .sortBy(status => status.getCreatedAt)
      .reverse

    val leastSimilarTweets : List[(Double, Status)] = topMatches
      .filter(tweet => taggedTweets.contains(tweet.getId) == false)
      .map(tweet => {
        val tweetTokens = targetTweets(tweet).toList

        // Compute Jaccard similarity
        var jaccardSim = 0.0
        for ( tokenSet <- taggedTweetTokens ) {
          val intersectionSize = tokenSet.intersect(tweetTokens).distinct.size
          val unionSize = (tokenSet ++ tweetTokens).distinct.size

          val localJaccardSim = intersectionSize.toDouble / unionSize.toDouble

          jaccardSim = Math.max(localJaccardSim, jaccardSim)
        }

        taggedTweets = taggedTweets + tweet.getId
        taggedTweetTokens = taggedTweetTokens :+ tweetTokens

        (jaccardSim, tweet)
      })
      .filter(tuple => tuple._1 <= SIM_THRESHOLD)
      .sortBy(tuple => tuple._1)
      .reverse
      .take(PER_MINUTE_MAX)

    leastSimilarTweets.map(tuple => {
      val tweet = tuple._2

      val lowerTweetText = tweet.getText.toLowerCase
      var topicIds = ""
      for (topic <- topicList) {
        breakable {
          for (token <- topic.tokens) {
            if (lowerTweetText.contains(token)) {
              topicIds += topic.num + "+"
              break
            }
          }
        }
      }

      val logEntry: String = createCsvString(topicIds, time, tweet.getId, tweet.getText)

      print(logEntry)

      logEntries = logEntries :+ logEntry
    })

    return logEntries
  }
}
