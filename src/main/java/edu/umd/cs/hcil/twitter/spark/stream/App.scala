/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.umd.cs.hcil.twitter.spark.stream

import java.io.FileWriter
import java.text.SimpleDateFormat

import scala.util.control.Breaks._
import org.apache.commons.csv.{CSVFormat, CSVPrinter}
import twitter4j.Status
import twitter4j.json.DataObjectFactory
import java.util.{Calendar, Date, Locale}

import scala.collection.JavaConverters._
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.commons.math3.linear.ArrayRealVector
import edu.umd.cs.hcil.twitter.spark.common.Conf
import edu.umd.cs.hcil.twitter.spark.common.ScoreGenerator
import edu.umd.cs.hcil.twitter.spark.scorers.RegressionScorer
import edu.umd.cs.hcil.twitter.spark.utils.StatusTokenizer

import scala.collection.immutable.Queue
import scala.collection.mutable
import org.json4s._
import org.json4s.jackson.JsonMethods._
import twitter4j.Status
import twitter4j.TwitterObjectFactory
import edu.umd.cs.hcil.twitter.streamer.TwitterUtils
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.memory.MemoryIndex
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser

import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

import play.api.libs.ws.ning.NingWSClient

object App {

  implicit val formats = DefaultFormats // Brings in default date formats etc.
  case class Topic(title: String, topid: String, description: String, narrative: String)

  // Construct an analyzer for our tweet text
  val localAnalyzer = new StandardAnalyzer()
  val localParser = new StandardQueryParser()

  // HTTP Client
  val wsClient = NingWSClient()

  /**
   * @param args the command line arguments
   */
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("Trec Real-Time Task")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))
    ssc.checkpoint("./checkpointDirectory")

    val propertiesPath = args(0)
    val topicsFile = args(1)
    val outputFile = args(2)

    val burstConf = new Conf(propertiesPath)

    var numTasks = 8
    if ( args.size > 3 ) {
      numTasks = args(3).toInt
    }

    val topicsJsonStr = scala.io.Source.fromFile(topicsFile).mkString
    val topicsJson = parse(topicsJsonStr)
    val topicList = topicsJson.extract[List[Topic]]
    val topicTitleList = topicList.map(topic => topic.title.toLowerCase)

    // If true, we use a socket. If false, we use the direct Twitter stream
    val replayOldStream = false

    // If we are going to use the direct twitter stream, use TwitterUtils. Else, use socket.
    val twitterStream = if ( replayOldStream == false ) {
      TwitterUtils.createStream(ssc, None)
    } else {
      val textStream = ssc.socketTextStream("localhost", 9999)
      textStream.map(line => {
        TwitterObjectFactory.createStatus(line)
      })
    }

    // Remove tweets not in English and other filters
    val noRetweetStream = twitterStream
      .filter(status => {
        status != null &&
        status.getLang != null &&
        status.getLang.compareToIgnoreCase("en") == 0 &&
        !status.getText.toLowerCase.contains("follow") &&
        status.getHashtagEntities.size <= burstConf.maxHashtags &&
        status.getURLEntities.size <= burstConf.maxUrls
    })

    // Only keep tweets that contain a topic token
    val topicalTweetStream = querier(topicTitleList, noRetweetStream, 0.0d)

    // Create pairs of statuses and tokens in those statuses
    val tweetTokenPairs = topicalTweetStream
      .map(status => {
          val tokens = StatusTokenizer.tokenize(status) ++ status.getHashtagEntities.map(ht => ht.getText)
          (status, tokens.map(str => str.toLowerCase))
        }).filter(tuple => tuple._2.size >= burstConf.minTokens)

    // Convert (tweet, tokens) to (user_id, tokenSet) to (token, 1)
    //  This conversion lets us count only one token per user
    val userCounts = tweetTokenPairs
      .map(pair => (pair._1.getUser.getId, pair._2.toSet))
      .reduceByKey(_ ++ _)
      .flatMap(pair => pair._2).map(token => (token, 1))

    val counts = userCounts
    val windowSum = counts.reduceByKeyAndWindow(
      (a:Int,b:Int) => (a + b), 
      Seconds(burstConf.minorWindowSize * 60),
      Seconds(60),
      numTasks
    )

    // Bursty keywords to look for in tweets
    var burstingKeywords : Queue[String] = Queue.empty
    
    // Build a slider of the last ten minutes
    var rddCount = 0
    var dateList : List[Date] = List.empty
    var rddList : List[RDD[Tuple2[String, Map[Date, Int]]]] = List.empty
    windowSum.foreachRDD((rdd, time) => {
        val dateTag = new Date(time.milliseconds)
        dateList = dateList :+ dateTag

        println("Window Count: " + rddCount)
        println("Dates so far: " + dateList)

        // Should be (token, Map[Date, Int])
        val datedPairs = rdd.map(tuple => (tuple._1, Map(dateTag -> tuple._2)))
        println("Date: " + dateTag.toString + ", Token Count: " + datedPairs.count())
        datedPairs.persist
        rddList = rddList :+ datedPairs
        
        val earliestDate = dateList(0)
        println("Earliest Date: " + earliestDate)
        
        // Merge all the RDDs in our list, so we have a full set of tokens that occur in this window
        val mergingRdd : RDD[Tuple2[String, Map[Date, Int]]] = rddList.reduce((rdd1, rdd2) => {
            rdd1 ++ rdd2
          })

        // Combine all the date maps for each token
        val combinedRddPre : RDD[Tuple2[String, Map[Date, Int]]] = mergingRdd.reduceByKey((a, b) => {
            a ++ b
          })

        val scores : RDD[Tuple2[String, Double]] = ScoreGenerator.scoreFrequencyArray(combinedRddPre, dateList)
        val sortedScores = scores.sortBy(tuple => tuple._2, false)

        val topList = sortedScores.take(20)
        println("\nPopular topics, Now: %s, Window: %s".format(new Date().toString, dateList.last.toString))
        topList.foreach{case (tag, score) => println("%s - %f".format(tag, score))}

        if ( rddCount >= burstConf.majorWindowSize ) {
          val targetKeywords = sortedScores
            .filter(tuple => tuple._1.length > 3)
            .filter(tuple => tuple._2 > burstConf.burstThreshold)
            .map(tuple => tuple._1).collect

          println("Over threshold count: " + targetKeywords.size)
          val topTokens : List[String] = targetKeywords.take(10).toList
          burstingKeywords = burstingKeywords.enqueue(topTokens)
          println("Bursting Keywords count: " + burstingKeywords.size)
        }
        
        // Prune the date and rdd lists as needed
        if ( dateList.size == burstConf.majorWindowSize ) {
          
          // Drop the earliest date
          dateList = dateList.slice(1, burstConf.majorWindowSize)
          
          // Drop the earliest RDD and unpersist it
          val earliestRdd = rddList.head
          rddList = rddList.slice(1, burstConf.majorWindowSize)
          earliestRdd.unpersist(false)
        }
        
        rddCount += 1
      })

    // Find tweets containing the bursty tokens
    val tweetWindowStream = tweetTokenPairs
      .window(
        Seconds(burstConf.majorWindowSize * 60),
        Seconds(60))

    var taggedTweets : Set[Long] = Set.empty
    var taggedTweetTokens : List[List[String]] = List.empty
    tweetWindowStream.foreachRDD((rdd, time) => {

      val tweetFinderStatus = future {
        println("Status RDD Time: " + time)
        val outputFileWriter = new FileWriter(outputFile, true)

        var capturedTweets: Map[Status, Int] = Map.empty

        println("Bursting Keyword Count: " + burstingKeywords.size + ", " + burstingKeywords.nonEmpty)

        var targetTokens: List[String] = List.empty
        while (burstingKeywords.nonEmpty) {
          val (token, newQ) = burstingKeywords.dequeue
          burstingKeywords = newQ

          targetTokens = targetTokens :+ token
        }
        println("Finding tweets containing: %s".format(targetTokens))

        val targetTweets = rdd.filter(tuple => {
          val status = tuple._1
          var flag = false

          for (token <- targetTokens) {
            if (status.getText.toLowerCase.contains(token)) {
              flag = true
            }
          }
          flag
        }).collect.toMap

        for (tweet <- targetTweets.keys) {
          capturedTweets = capturedTweets ++ Map(tweet -> (capturedTweets.getOrElse(tweet, 0) + 1))
        }

        val topMatches: List[Status] = capturedTweets
          .filter(tuple => tuple._2 == capturedTweets.values.max)
          .map(tuple => tuple._1)
          .toList
          .sortBy(status => status.getCreatedAt)

        val leastSimilarTweets : List[(Double, Status)] = topMatches
          .filter(tweet => taggedTweets.contains(tweet.getId) == false)
          .map(tweet => {
            val tweetTokens = targetTweets(tweet)

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

        // Match tweets to topics
        leastSimilarTweets.map(tuple => {
          val tweet = tuple._2
          val lowerTweetText = tweet.getText.toLowerCase

          // Construct an in-memory index for the tweet data
          val idx = new MemoryIndex()
          idx.addField("content", lowerTweetText, localAnalyzer)

          var topicIds = List[String]()

          for (topic <- topicList) {
            if ( idx.search(localParser.parse(topic.title.toLowerCase, "content")) > 0 ) {
              topicIds = topicIds :+ topic.topid

              // Submit tweet-topic to broker
              submitTweet(tweet, topic.topid, burstConf, wsClient)
            }
          }

          val topicString = topicIds.reduce((l, r) => l + "+" + r)

          val logEntry: String = createCsvString(topicString, time, tweet.getId, tweet.getText)

          print(logEntry)
          outputFileWriter.write(logEntry)
        })

        outputFileWriter.close()
      }

      tweetFinderStatus.onComplete {
        case Success(x) => println("Tweet finder SUCCESS")
        case Failure(ex) => println("Tweet finder FAILURE: " + ex.getMessage)
      }
    })
    
    ssc.start()
    ssc.awaitTermination()
  }
  
  def convertTimeToSlice(time : Date) : Date = {
    val cal = Calendar.getInstance
    cal.setTime(time)
    cal.set(Calendar.SECOND, 0)
    
    return cal.getTime
  }

  def createCsvString(topic : String, time : Time, tweetId : Long, text : String) : String = {
    val buff = new StringBuffer()
    val writer = new CSVPrinter(buff, CSVFormat.DEFAULT)

    writer.print(topic)
    writer.print(time.milliseconds / 1000)
    writer.print(tweetId)
    writer.print(text.replace("\n", " "))

    buff.toString + "\n"
  }

  def querier(queries : List[String], statusList : DStream[Status], threshold : Double) : DStream[Status] = {
    // Pseudo-Relevance feedback
    val scoredPairs = statusList.mapPartitions(iter => {
      // Construct an analyzer for our tweet text
      val analyzer = new StandardAnalyzer()
      val parser = new StandardQueryParser()

      // Use AND
      parser.setDefaultOperator(org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.Operator.AND)

      iter.map(status => {
        val text = status.getText

        // Construct an in-memory index for the tweet data
        val idx = new MemoryIndex()

        idx.addField("content", text.toLowerCase(), analyzer)

        var score = 0.0d
        for ( q <- queries ) {
          score = score + idx.search(parser.parse(q, "content"))
        }

        (status, score)
      })
    }).filter(tuple => tuple._2 > threshold)
      .map(tuple => tuple._1)

    return scoredPairs
  }

  def submitTweet(tweet : Status, topic : String, conf : Conf, wsClient : NingWSClient) : Unit = {

    // POST /tweet/:topid/:tweetid/:clientid
    val submitUrl = "%s/tweet/%s/%s/%s".format(conf.brokerUrl, topic, tweet.getId.toString, conf.clientId)

    wsClient
      .url(submitUrl)
      .execute("POST")
      .map( { wsResponse =>
        println(s"Submitted: ${submitUrl}")
        println(s"Status: ${wsResponse.status}")
        println(s"Body: ${wsResponse.body}")
      })

  }
}
