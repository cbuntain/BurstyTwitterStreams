/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.umd.cs.hcil.twitter.spark.stream

import java.io.FileWriter
import org.apache.commons.csv.{CSVFormat, CSVPrinter}
import twitter4j.Status
import java.util.{Calendar, Date}

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import edu.umd.cs.hcil.twitter.spark.common.Conf
import edu.umd.cs.hcil.twitter.spark.common.ScoreGenerator
import edu.umd.cs.hcil.twitter.spark.utils.StatusTokenizer

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

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashMap

import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

import play.api.libs.ws.ning.NingWSClient

object App {

  implicit val formats = DefaultFormats // Brings in default date formats etc.
  case class Topic(title: String, topid: String, description: String, narrative: String)
  case class TokenizedStatus(status : Status, tokens : List[String])

  // Construct an analyzer for our tweet text
  val localAnalyzer = new StandardAnalyzer()
  val localParser = new StandardQueryParser()

  // HTTP Client
  val wsClient = NingWSClient()

  // Configuration object
  var TrecBurstConf : Conf = null

  // Track queues for each topic
  val perTopicBurstThresholds : HashMap[String,Double] = HashMap.empty
  val perTopicBurstQueue : HashMap[String, Queue[String]] = HashMap.empty
  val perTopicTaggedTweets : HashMap[String, List[App.TokenizedStatus]] = HashMap.empty

  /**
   * @param args the command line arguments
   */
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("Trec Real-Time Task")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(30))
    ssc.checkpoint("./checkpointDirectory")

    val propertiesPath = args(0)
    val topicsFile = args(1)
    val outputFile = args(2)

    TrecBurstConf = new Conf(propertiesPath)
    val broad_BurstConf = sc.broadcast(TrecBurstConf)

    var numTasks = 16
    if ( args.size > 3 ) {
      numTasks = args(3).toInt
    }

    val topicsJsonStr = scala.io.Source.fromFile(topicsFile).mkString
    val topicsJson = parse(topicsJsonStr)
    val topicList = topicsJson.extract[List[Topic]]

    // Populate the per-topic maps, so we can track RDDs and bursts per topic
    for ( topic <- topicList ) {
      perTopicBurstThresholds(topic.topid) = TrecBurstConf.burstThreshold
      perTopicBurstQueue(topic.topid) = Queue.empty
      perTopicTaggedTweets(topic.topid) = List.empty
    }

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
          getHashtagCount(status) <= broad_BurstConf.value.maxHashtags &&
          getUrlCount(status) <= broad_BurstConf.value.maxUrls
    })

    // For each topic, process the twitter stream accordingly
    for ( topic <- topicList ) {

      // Only keep tweets that contain a topic token
      val topicalTweetStream = querier(List(topic.title), noRetweetStream, TrecBurstConf.queryThreshold)

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
        numTasks
      )

      // Checkpoint the windowSum RDD
      windowSum.checkpoint(Seconds(TrecBurstConf.majorWindowSize * 60))

      val stateSpec = StateSpec.function(mappingFunction _)
        .timeout(Seconds(60))

      val windowedState = windowSum.mapWithState(stateSpec)

      // Build a slider of the last few minutes
      var dateList: List[Date] = List.empty
      windowedState.foreachRDD((rdd, time) => {

        println("Processing Topic [%s]...".format(topic.topid))
        dateList = dateList :+ new Date(time.milliseconds)

        println("\tWindow Count: %d".format(dateList.length))

        // Filter the RDD to contain concrete values, and pass them to the
        //  scoring code
        val concrete = rdd.filter(tup => tup.nonEmpty).map(tup => tup.get)
        val tokenCount = concrete.count()
        println("\tNumber of tokens: %d".format(tokenCount))

        val scores : RDD[(String, Double)] = ScoreGenerator.scoreUndatedList(TrecBurstConf.majorWindowSize, concrete)

        if ( dateList.length >= TrecBurstConf.majorWindowSize ) {
          val topid = topic.topid

          val thisBurstThreshold = perTopicBurstThresholds(topid)
          val targetKeywords = scores
            .filter(tuple => tuple._1.length > 3)
            .filter(tuple => tuple._2 > thisBurstThreshold)
            .map(tuple => tuple._1).collect

          println("\tOver threshold count: " + targetKeywords.size)
          val topTokens : List[String] = targetKeywords.take(100).toList
          val newQueue = perTopicBurstQueue(topid).enqueue(topTokens)
          println("\tBursting Keywords count: " + newQueue.size)

          // Update this topic's queue
          perTopicBurstQueue(topid) = newQueue

          // Update the burst threshold for this topic if we have a lot of bursty tokens
          if ( topTokens.length > 10 ) {
            val newThresh = Math.sqrt(thisBurstThreshold)
            perTopicBurstThresholds(topid) = newThresh
            println("\tUpdating Threshold: " + thisBurstThreshold + " -> " + newThresh)
          }
        }

        // Drop the earliest date
        if (dateList.size == TrecBurstConf.majorWindowSize) {
          dateList = dateList.slice(1, TrecBurstConf.majorWindowSize)
        }

      })

      // Find tweets containing the bursty tokens
      val tweetWindowStream = tweetTokenPairs
        .window(
          Seconds(TrecBurstConf.minorWindowSize * 60),
          Seconds(60))

      // Checkpoint the RDD
      //tweetWindowStream.checkpoint(Seconds(TrecBurstConf.majorWindowSize * 60))

      // Process each tweet window
      tweetWindowStream.foreachRDD((rdd, time) => {

        // Use threading to avoid blocking on this topic
        val tweetFinderStatus = future {
          val burstyTweetCount = findImportantTweets(topic.topid, rdd, time, outputFile)
          println("Bursty Tweet Count: " + burstyTweetCount)

          burstyTweetCount
        }

        tweetFinderStatus.onComplete {
          case Success(x) => println("Tweet finder SUCCESS")
          case Failure(ex) => println("Tweet finder FAILURE: " + ex.getMessage)
        }

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
    val maxCount = targetTweets.map(t => t._2).max()

    // Get the tweets that appeared the most frequenly
    //  and order by their creation date
    val topMatches: List[(Status, List[String])] = targetTweets
      .filter(tuple => tuple._2 == maxCount)
      .map(tuple => (tuple._1, tuple._3))
      .sortBy(tuple => tuple._1.getCreatedAt)
      .collect()
      .toList

    // What tweets have we gotten for this topic before?
    var taggedTweets = perTopicTaggedTweets(topid)
    var taggedTweetIds = taggedTweets.map(tokedStatus => tokedStatus.status.getId)

    // Remove tweets that are similar to ones we've already pushed
    val similarityThreshold = TrecBurstConf.similarityThreshold
    val leastSimilarTweets: List[(Double, TokenizedStatus)] = topMatches
      .filter(tuple => taggedTweetIds.contains(tuple._1.getId) == false)
      .map(tuple => {
        val tweet = tuple._1
        val tokens = tuple._2

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

        (jaccardSim, tokenizedStatus)
      })
      .filter(tuple => tuple._1 <= similarityThreshold)
      .sortBy(tuple => tuple._1)
      .take(TrecBurstConf.perMinuteMax)

    val finalTweets = leastSimilarTweets.map(t => t._2)
    val pastTweets = perTopicTaggedTweets(topid)
    perTopicTaggedTweets(topid) = pastTweets ++ finalTweets

    // Match tweets to topics
    leastSimilarTweets.map(tuple => {
      val tweet = tuple._2
      val logEntry: String = createCsvString(topid, time, tweet.status.getId, tweet.status.getText)

      print(logEntry)
      outputFileWriter.write(logEntry)
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


}
