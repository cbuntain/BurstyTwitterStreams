package edu.umd.cs.hcil.twitter.spark.stream

import java.io.FileWriter
import java.util.{Calendar, Date}
import java.util.concurrent.Executors

import edu.umd.cs.hcil.twitter.spark.common.Conf
import edu.umd.cs.hcil.twitter.spark.utils.StatusTokenizer
import org.apache.commons.csv.{CSVFormat, CSVPrinter}
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.memory.MemoryIndex
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StateSpec, StreamingContext, Time}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._
import play.api.libs.ws.ning.NingWSClient
import twitter4j.{Status, TwitterObjectFactory}

import scala.collection.immutable.Queue
import scala.collection.mutable.{HashMap, ListBuffer}
import scala.concurrent._

/**
  * Created by cbuntain on 7/24/17.
  */
object RetweetApp {

  implicit val formats = DefaultFormats // Brings in default date formats etc.
  case class Topic(query: String, topid: String, description: String, narrative: String)
  case class TokenizedStatus(status : Status, tokens : List[String])

  // Construct an analyzer for our tweet text
  val localAnalyzer = new StandardAnalyzer()
  val localParser = new StandardQueryParser()

  // Burst log file
  val burstLogFileName = "trec_burst_thresholds.log"

  // Configuration object
  var TrecBurstConf : Conf = null

  // Track queues for each topic
  val perTopicBurstThresholds : HashMap[String,Double] = HashMap.empty
  val perTopicBurstQueue : HashMap[String, Queue[String]] = HashMap.empty
  val perTopicTaggedTweets : HashMap[String, List[TokenizedStatus]] = HashMap.empty

  // Create a thread pool for handling our analytics
  implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(20))

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

    var numTasks = 16
    if ( args.size > 3 ) {
      numTasks = args(3).toInt
    }

    val checkpointPath = if ( args.size > 4 ) {
      "./checkpointDirectory-" + args(4)
    } else {
      "./checkpointDirectory"
    }
    ssc.checkpoint(checkpointPath)

    TrecBurstConf = new Conf(propertiesPath)
    val broad_BurstConf = sc.broadcast(TrecBurstConf)

    val topicsJsonStr = scala.io.Source.fromFile(topicsFile).mkString
    val topicsJson = parse(topicsJsonStr)
    val topicList = topicsJson.extract[List[Topic]]

    // Populate the per-topic maps, so we can track RDDs and bursts per topic
    for ( topic <- topicList ) {
      perTopicBurstThresholds(topic.topid) = TrecBurstConf.retweetThreshold
      perTopicBurstQueue(topic.topid) = Queue.empty
      perTopicTaggedTweets(topic.topid) = List.empty
    }

    // If true, we use a socket. If false, we use the direct Twitter stream
    val replayOldStream = false

    // If we are going to use the direct twitter stream, use TwitterUtils. Else, use socket.
    val twitterStream = (if ( replayOldStream == false ) {
      TwitterUtils.createStream(ssc, None)
    } else {
      val textStream = ssc.socketTextStream("localhost", 9999)
      textStream.map(line => {
        TwitterObjectFactory.createStatus(line)
      })
    }).repartition(numTasks)



    // Remove tweets not in English and other filters
    val retweetStream = twitterStream
      .filter(status => {
        status != null &&
          status.getLang != null &&
          status.getLang.compareToIgnoreCase("en") == 0 &&
          status.isRetweet &&
          status.getRetweetedStatus.getRetweetCount >= broad_BurstConf.value.retweetThreshold &&
          !status.getText.toLowerCase.contains("follow") &&
          getHashtagCount(status) <= broad_BurstConf.value.maxHashtags &&
          getUrlCount(status) <= broad_BurstConf.value.maxUrls
      })
    retweetStream.cache()

    // For each topic, process the twitter stream accordingly
    for ( topic <- topicList ) {

      // Only keep tweets that contain a topic token
      val topicalTweetStream = querier(List(topic.query), retweetStream, TrecBurstConf.queryThreshold)

      // Process each tweet window
      topicalTweetStream.foreachRDD((rdd, time) => {

        // Use threading to avoid blocking on this topic
        val tweetFinderStatus = future {
          val threshold = perTopicBurstThresholds(topic.topid)
          val higherRetweetRdd = rdd.filter(t => t.getRetweetedStatus.getRetweetCount >= threshold)

          val burstyTweetCount = findImportantTweets(topic.topid, higherRetweetRdd, time, outputFile)
          if ( burstyTweetCount > 0 ) {
            println(topic.topid + " - Bursty Tweet Count: " + burstyTweetCount + "\n")
          }

          burstyTweetCount
        }
      })
    }

    ssc.start()
    ssc.awaitTermination()
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


  def createCsvString(topic : String, time : Time, tweetId : Long, text : String) : String = {
    val buff = new StringBuffer()
    val writer = new CSVPrinter(buff, CSVFormat.DEFAULT)

    writer.print(topic)
    writer.print(time.milliseconds / 1000)
    writer.print(tweetId)
    writer.print(text.replace("\n", " "))

    buff.toString + "\n"
  }

  def findImportantTweets(topid : String, rdd : RDD[Status], time : Time, outputFile : String) : Int = {

    println("Status RDD Time: " + time)
    val outputFileWriter = new FileWriter(outputFile, true)

    // If we have no bursty tokens or no tweets, skip
    val tweetCount = rdd.count()
    if ( tweetCount == 0 ) {
      return 0
    }

    // Update threshold
    if ( tweetCount > 10 ) {
      val oldThresh = perTopicBurstThresholds(topid)
      val newThresh = oldThresh * 10
      perTopicBurstThresholds(topid) = newThresh
      outputFileWriter.write("Topic: %s, Tweet Count: %d, Old Threshold: %f, New Threshold: %f\n".format(
        topid,
        tweetCount,
        oldThresh,
        newThresh
      ))
    }

    val maxCount = rdd.map(t => t.getRetweetedStatus.getRetweetCount).max()

    // Get the tweets that appeared the most frequenly
    //  and order by their creation date
    val topMatches: List[Status] = rdd
      .filter(t => t.getRetweetedStatus.getRetweetCount == maxCount)
      .sortBy(t => t.getCreatedAt)
      .collect()
      .toList

    // What tweets have we gotten for this topic before?
    var taggedTweets = perTopicTaggedTweets(topid)
    var taggedTweetIds = taggedTweets.map(tokedStatus => tokedStatus.status.getId)

    // Remove tweets that are similar to ones we've already pushed
    val similarityThreshold = TrecBurstConf.similarityThreshold
    val leastSimilarTweets: List[(Double, TokenizedStatus)] = topMatches
      .filter(t => taggedTweetIds.contains(t.getId) == false)
      .map(t => {
        val tweet = t
        val tokens = StatusTokenizer.tokenize(tweet)

        // Compute Jaccard similarity
        var jaccardSim = 0.0
        for (priorTweet <- taggedTweets) {
          val tokenSet = priorTweet.tokens

          val intersectionSize = tokenSet.intersect(tokens).distinct.size
          val unionSize = (tokenSet ++ tokens).distinct.size

          val localJaccardSim = intersectionSize.toDouble / unionSize.toDouble

          jaccardSim = Math.max(localJaccardSim, jaccardSim)
        }

        val tokenizedStatus = new TokenizedStatus(status=tweet, tokens=tokens)
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

    // HTTP Client
    val wsClient = NingWSClient()

    // Match tweets to topics
    leastSimilarTweets.foreach(tuple => {
      val tweet = tuple._2
      val logEntry: String = createCsvString(topid, time, tweet.status.getId, tweet.status.getText)

      print(logEntry)
      outputFileWriter.write(logEntry)

      future { submitTweet(tweet.status, topid, TrecBurstConf, wsClient) }
    })

    outputFileWriter.close()

    return finalTweets.length
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
