/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.umd.cs.hcil.twitter.spark.learners

import java.io.FileWriter
import java.util.Date

import scala.io.Source
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

object BatchTweetFinder {

  implicit val formats = DefaultFormats // Brings in default date formats etc.
  case class Topic(title: String, num: String, tokens: List[String])

  // Record all tweets we tag
  var taggedTweets : Set[Long] = Set.empty
  var taggedTweetTokens : List[List[String]] = List.empty

  /**
   * @param args the command line arguments
   */
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("TREC Offline Tweet Finder")
    val sc = new SparkContext(conf)

    val propertiesPath = args(0)
    val dataPath = args(1)
    val keywordFile = args(2)
    val topicsFile = args(3)
    val outputFile = args(4)

    val burstConf = new Conf(propertiesPath)
    val burstyKeywordMap = parseKeywordFile(keywordFile)
    val datesWithKeywords = burstyKeywordMap.keys.filter(k => {
      burstyKeywordMap(k).filter(tuple => {
        tuple._2 > burstConf.burstThreshold
      }).size > 0
    }).toList.sorted

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

    // All times we need to process
    var timeRanges : List[Date] = List.empty

    /* TODO:::::
     *
     * Fix this. We're starting too early somehow and not getting the second dates
     *
     *
     */

    // Iterate through the dates we know have bursty keywords
    for ( burstTime <- datesWithKeywords ) {
      val dateIndex = fullKeyList.indexOf(burstTime)

      // I realize it should be + 1, but I have a consistent error in the OfflineApp
      //  that has an off-by-one problem as well, so I'm doing this for consistency.
      // val firstIndex = dateIndex - burstConf.majorWindowSize + 1
      val firstIndex = dateIndex - burstConf.majorWindowSize

      println("Burst Time: " + burstTime)

      val dateRange = fullKeyList.slice(firstIndex, dateIndex + 1)
      println("Start Time: " + dateRange.head)
      println("End Time: " + dateRange.last)

      timeRanges = timeRanges ++ dateRange
    }

    // Prune duplicates
    timeRanges = timeRanges.distinct.sorted

    var dateRanges : List[(Int, Int)] = List.empty

    var firstTime = timeRanges.head
    for (i <- timeRanges.indices.drop(1)) {
      val lastTime = timeRanges(i-1)
      val thisTime = timeRanges(i)

      val lastTimeIndex = fullKeyList.indexOf(lastTime)
      val thisTimeIndex = fullKeyList.indexOf(thisTime)

      if ( lastTimeIndex + 1 != thisTimeIndex ) {
        val firstTimeIndex = fullKeyList.indexOf(firstTime)
        dateRanges = dateRanges :+ (firstTimeIndex, lastTimeIndex + 1)

        firstTime = thisTime
      }
    }

    for ( dateRangeTuple <- dateRanges ) {

      val dateRange = fullKeyList.slice(dateRangeTuple._1, dateRangeTuple._2)
      println("Date Range:")
      println("\t" + dateRange.head + " - " + dateRange.last)

      // Build a slider of the last MAJOR_WINDOW_SIZE minutes
      var rddCount = 0
      var dateList: List[Date] = List.empty
      var tweetRddList: List[RDD[(Status, List[String])]] = List.empty
      for ( time <- dateRange ) {
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
          val tweetText = status.getText

          try {
            val tokens = StatusTokenizer.tokenize(status) ++ status.getHashtagEntities.map(ht => ht.getText)
            (status, tokens.map(str => str.toLowerCase).toList)
          } catch {
            case iae : IllegalArgumentException => {
              println(s"Tweet [$tweetText] caused tokenizer to fail with illegal argument.")
              (status, List.empty)
            }
            case e : Exception => {
              println(s"Tweet [$tweetText] caused tokenizer to fail with exception: " + e.toString)
              (status, List.empty)
            }
          }
        }).filter(tuple => tuple._2.size >= burstConf.minTokens)
        tweetTokenPairs.persist()
        tweetRddList = tweetRddList :+ tweetTokenPairs

        val scores: List[(String, Double)] = if ( burstyKeywordMap.keySet.contains(dateTag) ) {
          burstyKeywordMap(dateTag).toList
        } else {
          List.empty
        }
        val sortedScores = scores.sortBy(tuple => tuple._2).reverse

        val topList = sortedScores.take(20)
        println("\nPopular topics, Now: %s, Window: %s".format(new Date().toString, dateList.last.toString))
        topList.foreach { case (tag, score) => println("%s - %f".format(tag, score)) }

        // Bursty keywords to look for in tweets
        var burstingKeywords : List[String] = List.empty

        // Only look for bursty tokens if we're beyond the major window size
        if (rddCount >= burstConf.majorWindowSize) {
          val targetKeywords = sortedScores
            .filter(tuple => tuple._1.length > 3)
            .filter(tuple => tuple._2 > burstConf.burstThreshold)
            .map(tuple => tuple._1)

          println("Over threshold count: " + targetKeywords.size)
          val topTokens: List[String] = targetKeywords.take(10)

          burstingKeywords = burstingKeywords ++ topTokens
          println("Bursting Keywords count: " + burstingKeywords.size)
        }

        // Find the best tweets containing the top tokens and write to output file
        val outputFileWriter = new FileWriter(outputFile, true)
        val logEntries = findGoodTweets(time, burstingKeywords, tweetRddList, topicList, burstConf)
        logEntries.foreach(logEntry => outputFileWriter.write(logEntry))
        outputFileWriter.close()

        // Prune the date and rdd lists as needed
        if (dateList.size == burstConf.majorWindowSize) {

          // Drop the earliest date
          dateList = dateList.slice(1, burstConf.majorWindowSize)

          // Drop the earliest tweet RDD as well
          val earliestTweetRdd = tweetRddList.head
          tweetRddList = tweetRddList.slice(1, burstConf.majorWindowSize)
          earliestTweetRdd.unpersist(false)
        }

        rddCount += 1
      }
    }
  }

  case class Keywords(date: Long, pairs: Map[String, Double])

  def parseKeywordFile(keywordFilePath : String) : Map[Date, Map[String, Double]] = {

    var dateMap : Map[Date, Map[String, Double]] = Map.empty

    for (line <- Source.fromFile(keywordFilePath).getLines()) {
      val json = parse(line)
      val keywordMap = json.extract[Keywords]

      val thisDate : Date = new Date(keywordMap.date)
      val pairs : Map[String, Double] = keywordMap.pairs

      dateMap = dateMap ++ Map(thisDate -> pairs)
    }

    return dateMap
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
                      topicList : List[BatchTweetFinder.Topic],
                      burstConf : Conf) : List[String] = {

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
      .filter(tuple => tuple._1 <= burstConf.similarityThreshold)
      .sortBy(tuple => tuple._1)
      .reverse
      .take(burstConf.perMinuteMax)

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
