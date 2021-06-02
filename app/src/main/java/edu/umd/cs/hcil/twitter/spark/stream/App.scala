/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.umd.cs.hcil.twitter.spark.stream

import java.io.FileWriter
import java.util.concurrent.Executors

import org.apache.commons.csv.{CSVFormat, CSVPrinter}
import java.util.{Calendar, Date}

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import edu.umd.cs.hcil.twitter.spark.common.Conf
import edu.umd.cs.hcil.twitter.spark.common.ScoreGenerator
import edu.umd.cs.hcil.twitter.spark.utils.StatusTokenizer

import edu.umd.cs.hcil.twitter.streamer.KafkaTwitterStream

import scala.collection.immutable.Queue
import org.json4s._
import org.json4s.jackson.JsonMethods._
import twitter4j.Status
import twitter4j.TwitterObjectFactory
import org.apache.spark.streaming.twitter._
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.memory.MemoryIndex
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler

import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.util.{Failure, Success}
import play.api.libs.ws.ning.NingWSClient

object App {

  implicit val formats = DefaultFormats // Brings in default date formats etc.
  case class Topic(query: String, topid: String, description: String, narrative: String)
  case class TokenizedStatus(status : Status, tokens : List[String])

  // Construct an analyzer for our tweet text
  val localAnalyzer = new StandardAnalyzer()
  val localParser = new StandardQueryParser()

  // Burst log file
  val burstLogFileName = "trec_burst_thresholds.log"
  val burstTokenLogFileName = "trec_burst_tokens.log"

  // Configuration object
  var TrecBurstConf : Conf = null

  // Track queues for each topic
  val perTopicBurstThresholds : HashMap[String,Double] = HashMap.empty
  val perTopicBurstQueue : HashMap[String, Queue[String]] = HashMap.empty
  val perTopicTaggedTweets : HashMap[String, List[App.TokenizedStatus]] = HashMap.empty

  // HTTP Client
//  @transient lazy val wsClient = NingWSClient()
    val wsClient = NingWSClient()

  // Create a thread pool for handling our analytics
//  implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(20))

  /**
   * @param args the command line arguments
   */
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("Trec Real-Time Task")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(30))

    val propertiesPath = args(0)
    val topicsFile = args(1)
    val outputFile = args(2)
    val filtersFile = args(3)

    var numTasks = 16

    val checkpointPath = {
      "./checkpointDirectory"
    }
    ssc.checkpoint(checkpointPath)

    val filters = scala.io.Source.fromFile(filtersFile).mkString.split("\n")

    TrecBurstConf = new Conf(propertiesPath)
    val broad_BurstConf = sc.broadcast(TrecBurstConf)

//    val burstLogger = Logger.getLogger("BurstThreshold")
//    val logHandler = new FileHandler(burstLogFileName, true)
//    logHandler.setFormatter(new SimpleFormatter())
//    burstLogger.addHandler(logHandler)

    val topicsJsonStr = scala.io.Source.fromFile(topicsFile).mkString
    val topicsJson = parse(topicsJsonStr)
    val topicList = topicsJson.extract[List[Topic]]

    // Populate the per-topic maps, so we can track RDDs and bursts per topic
    for ( topic <- topicList ) {
      perTopicBurstThresholds(topic.topid) = TrecBurstConf.burstThreshold
      perTopicBurstQueue(topic.topid) = Queue.empty
      perTopicTaggedTweets(topic.topid) = List.empty
    }

    // If we are going to use the direct twitter stream, use TwitterUtils. Else, use socket.
    val twitterStream = (
      if ( TrecBurstConf.useReplay.equalsIgnoreCase("true") ) {
        val textStream = ssc.socketTextStream("localhost", 9999)
        textStream.map(line => {
          TwitterObjectFactory.createStatus(line)
        })
      } else if (TrecBurstConf.useKafka.equalsIgnoreCase("true") ) {
        KafkaTwitterStream.getStream(ssc, TrecBurstConf)
      } else {
        TwitterUtils.createStream(ssc, None, filters)
      }
    ).repartition(numTasks)

    // Remove tweets not in English and other filters
    val noRetweetStream = twitterStream
      .filter(status => {
        // status != null &&
        // status.getLang != null &&
        // status.getLang.compareToIgnoreCase("en") == 0 &&
        !status.getText.toLowerCase.contains("follow") &&
          getHashtagCount(status) <= broad_BurstConf.value.maxHashtags &&
          getUrlCount(status) <= broad_BurstConf.value.maxUrls
    })
    noRetweetStream.cache()

    // For each topic, process the twitter stream accordingly
    for ( topic <- topicList ) {

      /l Only keep tweets that contain a topic ooken
      val topicalTweetStream = querier(List(topic.query), noRetweetStream, TrecBurstConf.queryThreshold)

      // Create pairs of statuses and tokens in those statuses
      val tweetTokenPairs = topicalTweetStream
        .map(status => {
          val tokens = StatusTokenizer.tokenize(status) ++ status.getHashtagEntities.map(ht => ht.getText)
          (status, tokens.map(str => str.toLowerCase))
        }).filter(tuple => tuple._2.size >= broad_BurstConf.value.minTokens)

      // Convert (tweet, tokens) to (user_id, tokenSet) to (token, 1)
      //  This conversion lets us count only one token per user
      val userCounts = tweetTokenPairs
        .map(pair => (pair._1.getUser.getId, pair._2.toSet))
        .reduceByKey(_ ++ _)
        .flatMap(pair => pair._2)
        .map(token => (token, 1))

      val counts = userCounts

      // Generate sliding window sums that merge counts for each token
      //  across some number of minutes
      //  NOTE: The inv func removes keys as they leave the window
      val windowSum = counts.reduceByKeyAndWindow(
        (a: Int, b: Int) => (a + b),
        (a: Int, b: Int) => (a - b),
        Seconds(TrecBurstConf.minorWindowSize * 60),
        Seconds(60),
        numTasks/2
      )

      // Checkpoint the windowSum RDD
      windowSum.checkpoint(Seconds(TrecBurstConf.minorWindowSize * 60))
      windowSum.cache()

      val stateSpec = StateSpec.function(mappingFunction _)
        .timeout(Seconds(60))

      val windowedState = windowSum.mapWithState(stateSpec)

      // Build a slider of the last few minutes
      var dateList: List[Date] = List.empty
      val burstyTokenCount : ListBuffer[Int] = ListBuffer.empty
      windowedState.foreachRDD((rdd, time) => {

        {
          val topid = topic.topid
          println("Processing Topic [%s]...".format(topid))
          dateList = dateList :+ new Date(time.milliseconds)

          println("\t" + topid + " - Window Count: %d".format(dateList.length))

          // Filter the RDD to contain concrete values, and pass them to the
          //  scoring code
          val concrete = rdd.filter(tup => tup.nonEmpty).map(tup => tup.get)
          val tokenCount = concrete.count()

          if (tokenCount > 0) {

            println("\t" + topid +  " - Number of tokens: %d".format(tokenCount))
            val scores: RDD[(String, Double)] = ScoreGenerator.scoreUndatedList(TrecBurstConf.majorWindowSize, concrete)

            if (dateList.length >= TrecBurstConf.majorWindowSize) {

              val thisBurstThreshold = perTopicBurstThresholds(topid)
              val targetKeywordScores = scores
                .filter(tuple => tuple._1.length > 3)
                .filter(tuple => tuple._2 > thisBurstThreshold)
                .collect
              val targetKeywords = targetKeywordScores.map(tuple => tuple._1)

              // Update the number of bursty tokens we've seen
              burstyTokenCount.append(targetKeywords.length)

              if (targetKeywords.size > 0) {
                println("\t" + topid + " - Over threshold count: " + targetKeywords.size)
                val topTokens: List[String] = targetKeywords.take(100).toList
                val newQueue = perTopicBurstQueue(topid).enqueue(topTokens)
                println("\t" + topid + " - Bursting Keywords count: " + newQueue.size)

                storeBurstyKeywords(topid, targetKeywordScores.take(100).toList, time.milliseconds)

                // Update this topic's queue
                perTopicBurstQueue(topid) = newQueue

                // Update the burst threshold for this topic if we have a lot of bursty tokens
                if (topTokens.length > 10) {
                  val newThresh = Math.pow(thisBurstThreshold, TrecBurstConf.thresholdModifier)
                  updateBurstThreshold(newThresh, thisBurstThreshold, topid, topTokens.length)
                }
              } else {

                // Should we ease the burst threshold?
                //  Look at the past few times we had relevant tokens to see how many
                //  times we found zero bursty keywords
                if ( burstyTokenCount.length > 3*TrecBurstConf.majorWindowSize ) {
                  val burstySum = burstyTokenCount.takeRight(3*TrecBurstConf.majorWindowSize).reduce(_ + _)

                  // Haven't had a bursty token in a while, so ease burst threshold?
                  if ( burstySum == 0 ) {
                    val newThresh = Math.pow(thisBurstThreshold, 1.0/TrecBurstConf.thresholdModifier)

                    // Don't want to drop below the minimum threshold with which we start
                    if ( newThresh >= TrecBurstConf.burstThreshold ) {
                      updateBurstThreshold(newThresh, thisBurstThreshold, topid, 0)
                    }
                  }
                }

              }
            }

            // Drop the earliest date
            if (dateList.size == TrecBurstConf.majorWindowSize) {
              dateList = dateList.slice(1, TrecBurstConf.majorWindowSize)
            }
          }
        }

      })

      // Find tweets containing the bursty tokens
      val tweetWindowStream = tweetTokenPairs
        .window(
          Seconds(TrecBurstConf.minorWindowSize * 60),
          Seconds(60))

      //

      // Checkpoint the RDD
      //tweetWindowStream.checkpoint(Seconds(TrecBurstConf.majorWindowSize * 60))

      // Process each tweet window
      tweetWindowStream.foreachRDD((rdd, time) => {

        // Use threading to avoid blocking on this topic
        // val tweetFinderStatus = future {
        val tweetFinderStatus = {
          val burstyTweetCount = findImportantTweets(topic.topid, rdd, time, outputFile)
          if ( burstyTweetCount > 0 ) {
            println(topic.topid + " - Bursty Tweet Count: " + burstyTweetCount + "\n")
          }

          burstyTweetCount
        }

//        tweetFinderStatus.onComplete {
//          case Success(x) => println("Tweet finder SUCCESS")
//          case Failure(ex) => println("Tweet finder FAILURE: " + ex.getMessage)
//        }

      })
    }
    
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

      // Use OR
      parser.setDefaultOperator(StandardQueryConfigHandler.Operator.OR)

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

  def submitTweet(tweet : Status, jaccardSim : Double, tweetBurstCount : Int, topic : String, conf : Conf, wsClient : NingWSClient) : Unit = {

    // POST /tweet/:topid/:tweetid/:clientid
    val submitUrl = "%s/tweet/%s/%s/%s".format(conf.brokerUrl, topic, tweet.getId.toString, conf.clientId)

    println(s"***** Submitting via POST: $submitUrl")
    val jsonString = """{"id":""" + tweet.getId.toString + """, "id_str":"""" + tweet.getId.toString + """", "jaccardSim": """ + s"${jaccardSim}, " + """"tweetBurstCount": """ + s"${tweetBurstCount}}"

    wsClient
      .url("http://submit_server:8080")
      .post(jsonString)
      .map( { wsResponse =>
        println(s"Submitted: ${submitUrl}")
        println(s"Status: ${wsResponse.status}")
        println(s"Body: ${wsResponse.body}")
      })

  }

  def getHashtagCount(status : Status) : Int = {
    val count = if ( status.getHashtagEntities != null ) {
      status.getHashtagEntities.size
    }  else {
      0
    }

    return count
  }

  def getUrlCount(status : Status) : Int = {
    val count = if ( status.getURLEntities != null ) {
      status.getURLEntities.size
    }  else {
      0
    }

    return count
  }

  def findImportantTweets(topid : String, rdd : RDD[(Status, List[String])], time : Time, outputFile : String) : Int = {

    println("Status RDD Time: " + time)
    val outputFileWriter = new FileWriter(outputFile, true)

    // Get the list of bursting keywords for this topic
    val targetTokens: List[String] = perTopicBurstQueue(topid).toList
    perTopicBurstQueue(topid) = Queue.empty
    println("Bursting Keyword Count: " + targetTokens.size)
    println("Finding tweets containing: %s".format(targetTokens))

    // If we have no bursty tokens or no tweets, skip
    val tweetCount = rdd.count()
    if ( tweetCount == 0 || targetTokens.size == 0 ) {
      println("Empty RDD or no tokens...")
      println("\tRDD Count: %d, Token Count: %d".format(tweetCount, targetTokens.size))
      return 0
    }

    // Count the frequency of bursty keywords in each tweet
    //  and only keep those tweets that contain such a keyword
    val targetTweets = rdd.map(tuple => {
      val status = tuple._1
      val tokens = tuple._2
      val count : Int = targetTokens.map(token => if (status.getText.toLowerCase.contains(token)) { 1 } else { 0 } ).reduce(_ + _)

      (status, count, tokens)
    }).filter(t => t._2 > 0)

    if ( targetTweets.isEmpty ) {
      println("No status contains target tokens...")
      return 0
    }

    val maxCount = targetTweets.map(t => t._2).max()

    // Get the tweets that appeared the most frequenly
    //  and order by their creation date
    val topMatches: List[(Status, List[String], Int)] = targetTweets
      .filter(tuple => tuple._2 == maxCount)
      .map(tuple => (tuple._1, tuple._3, tuple._2))
      .sortBy(tuple => tuple._1.getCreatedAt)
      .collect()
      .toList

    println(s"Match Count: ${topMatches.size}")

    // What tweets have we gotten for this topic before?
    var taggedTweets = perTopicTaggedTweets(topid)
    var taggedTweetIds = taggedTweets.map(tokedStatus => tokedStatus.status.getId)

    // Remove tweets that are similar to ones we've already pushed
    val similarityThreshold = TrecBurstConf.similarityThreshold
    val leastSimilarTweets: List[(Double, Int, TokenizedStatus)] = topMatches
      .filter(tuple => taggedTweetIds.contains(tuple._1.getId) == false)
      .map(tuple => {
        val tweet = tuple._1
        val tokens = tuple._2
        val tweetBurstCount = tuple._3

        // Compute Jaccard similarity
        var jaccardSim = 0.0
        for (priorTweet <- taggedTweets) {
          val tokenSet = priorTweet.tokens

          val intersectionSize = tokenSet.intersect(tokens).distinct.size
          val unionSize = (tokenSet ++ tokens).distinct.size

          val localJaccardSim = intersectionSize.toDouble / unionSize.toDouble

          jaccardSim = Math.max(localJaccardSim, jaccardSim)
        }

        val tokenizedStatus = new App.TokenizedStatus(status=tweet, tokens=tokens)
        taggedTweets = taggedTweets :+ tokenizedStatus
        taggedTweetIds = taggedTweetIds :+ tweet.getId

        (jaccardSim, tweetBurstCount, tokenizedStatus)
      })
      .filter(tuple => tuple._1 <= similarityThreshold)
      .sortBy(tuple => tuple._1)
      .take(TrecBurstConf.perMinuteMax)

    println("**** Total Count: " + leastSimilarTweets.size)
    val finalTweets = leastSimilarTweets.map(t => t._3)
    val pastTweets = perTopicTaggedTweets(topid)
    perTopicTaggedTweets(topid) = pastTweets ++ finalTweets

    // Match tweets to topics
    leastSimilarTweets.foreach(tuple => {
      val jaccardSim = tuple._1
      val tweetBurstCount = tuple._2
      val tweet = tuple._3
      val logEntry: String = createCsvString(topid, time, tweet.status.getId, tweet.status.getText)

      print(logEntry)
      outputFileWriter.write(logEntry)
      submitTweet(tweet.status, jaccardSim, tweetBurstCount, topid, TrecBurstConf, wsClient)
    })

    outputFileWriter.close()

    return finalTweets.length
  }


  // A mapping function that maintains an integer state and returns a String
  def mappingFunction(key: String, value: Option[Int], state: State[List[Int]]): Option[(String, List[Int])] = {

    // Check if state exists
    val freqListOption : Option[List[Int]] = if (state.exists) {

      val existingState = state.get  // Get the existing state

      // Decide whether to remove the state based on whether it is empty
      val shouldRemove = existingState.sum == 0

      if (shouldRemove) {
        state.remove()     // Remove the state

        None
      } else {
        val newState = existingState.takeRight(50) :+ value.getOrElse(0)
        state.update(newState)    // Set the new state

        Some(newState)
      }
    } else {

      val initialState = List(value.getOrElse(0))
      state.update(initialState)  // Set the initial state

      Some(initialState)
    }


    if ( freqListOption.nonEmpty ) {
      Some((key, freqListOption.get))
    } else {
      None
    }
  }


  // For updating burst thresholds
  def updateBurstThreshold(newThresh : Double, oldThresh : Double, topid : String, burstCount : Int): Unit = {
    perTopicBurstThresholds(topid) = newThresh
    println("\t[%s] - Updating Threshold: %f -> %f".format(topid, oldThresh, newThresh))

    val outputFileWriter = new FileWriter(burstLogFileName, true)
    outputFileWriter.write("Topic: %s, Bursty Token Count: %d, Old Threshold: %f, New Threshold: %f\n".format(
      topid,
      burstCount,
      oldThresh,
      newThresh
    ))
    outputFileWriter.close()
  }

  // For updating burst thresholds
  def storeBurstyKeywords(topid : String, keywords : List[(String,Double)], time : Long): Unit = {

    val outputFileWriter = new FileWriter(burstTokenLogFileName, true)
    for ( keywordTup <- keywords ) {
      outputFileWriter.write("%s,%s,%f,%d\n".format(
        topid,
        keywordTup._1,
        keywordTup._2,
        time
      ))
    }
    outputFileWriter.close()
  }
}
