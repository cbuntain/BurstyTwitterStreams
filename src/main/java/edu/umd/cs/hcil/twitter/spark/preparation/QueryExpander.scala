package edu.umd.cs.hcil.twitter.spark.preparation

import org.apache.lucene.analysis.core.{StopAnalyzer, SimpleAnalyzer}
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.memory.MemoryIndex
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser
import org.apache.lucene.queryparser.simple.SimpleQueryParser

import java.io.FileWriter
import java.io.File
import java.io.BufferedWriter

import java.util.Date

import edu.umd.cs.hcil.twitter.spark.common.{Conf, ScoreGenerator}
import edu.umd.cs.hcil.twitter.spark.utils.DateUtils
import org.apache.commons.csv.{CSVFormat, CSVPrinter}
import org.apache.spark._
import org.apache.spark.mllib.feature.Word2Vec
import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import twitter4j.{Status, TwitterObjectFactory, TwitterException}
import edu.umd.cs.hcil.twitter.spark.utils.StatusTokenizer

import scala.collection.JavaConverters._
import scala.util.control.Breaks._

/**
 * Created by cbuntain on 2/25/16.
 */
object QueryExpander {

  implicit val formats = DefaultFormats // Brings in default date formats etc.
  case class Topic(query: String, topid: String, description: String, narrative: String)



  /**
   * @param args the command line arguments
   */
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("TREC Query Expansion")
    val sc = new SparkContext(conf)

    val propertiesPath = args(0)
    val inputData = args(1)
    val topicsFile = args(2)
    val outputFile = args(3)

    val burstConf = new Conf(propertiesPath)

    val stopWords = StopAnalyzer.ENGLISH_STOP_WORDS_SET

    val twitterMsgsRaw = sc.textFile(inputData)
    println("Initial Partition Count: " + twitterMsgsRaw.partitions.size)

    var twitterMsgs = twitterMsgsRaw
    if (args.size > 4) {
      val initialPartitions = args(4).toInt
      twitterMsgs = twitterMsgsRaw.repartition(initialPartitions)
      println("New Partition Count: " + twitterMsgs.partitions.size)
    }
    val newPartitionSize = twitterMsgs.partitions.size

    // Convert each JSON line in the file to a status using Twitter4j
    //  Note that not all lines are Status lines, so we catch any exception
    //  generated during this conversion and set to null since we don't care
    //  about non-status lines.'
    val tweets = twitterMsgs.map(line => {
      try {
        TwitterObjectFactory.createStatus(line)
      } catch {
        case e : Exception => null
      }
    }).filter(status => status != null)
      .filter(status => status.getLang == "en")
      .filter(status => status.isRetweet == false)

    val tweetTextTuples = tweets.map(status => {
      val tokens : List[String] = if ( burstConf.makeBigrams ) {
        buildBigrams(StatusTokenizer.tokenize(status)).filter(token => {
          token.length >= burstConf.queryExpMinTokenSize
        })
      } else {
        StatusTokenizer.tokenize(status).filter(token => {
          token.length >= burstConf.queryExpMinTokenSize
        })
      }

      (status.getId,
        status.getText,
        tokens.map(token => token.toLowerCase),
        status.getHashtagEntities.map(entity => entity.getText.toLowerCase))
    })
    println("Tweet Count: %d".format(tweetTextTuples.count))

    val topicsJsonStr = scala.io.Source.fromFile(topicsFile).mkString
    val topicsJson = parse(topicsJsonStr)
    val topicList = topicsJson.extract[List[Topic]]

    println("Searching for:")
    for (topic <- topicList) {
      println(topic.topid, topic.query)
    }

    tweetTextTuples.cache()

    // Build the background model from all tweets
    val bgModelCounts : RDD[(String, Int)] = tweetTextTuples.flatMap(tuple => {
      tuple._3.map(token => (token, 1))
    }).reduceByKey((l, r) => l + r)
    val bgModelTotal = bgModelCounts.map(tuple => tuple._2).reduce((l, r) => l + r)
    val logTotal = math.log(bgModelTotal)
    val bgModel = bgModelCounts.mapValues(count => math.log(count) - logTotal).collect().toMap

    // New topic set
    var newTopicList : List[Topic] = List.empty

    // Build the foregound model for each topic
    for ( topic <- topicList ) {
      val strippedTitle = topic.query

      // Pseudo-Relevance feedback
      val scoredPairs_ = tweetTextTuples.mapPartitions(iter => {
        // Construct an analyzer for our tweet text
        val analyzer = new StandardAnalyzer()
        val parser = new StandardQueryParser()

        iter.map(pair => {
          val id = pair._1
          val text = pair._2
          val tokens = pair._3
          val hashtags = pair._4

          // Construct an in-memory index for the tweet data
          val idx = new MemoryIndex()

          idx.addField("content", text.toLowerCase(), analyzer)

          val score = idx.search(parser.parse(strippedTitle, "content"))

          (id, text, tokens, hashtags, score)
        })
      })
      val scores_ = scoredPairs_.map(tuple => tuple._5).collect
      println("Max: %f".format(scores_.max))
      println("Mean: %f".format(scores_.reduce(_+_) / scores_.length))
      
      val scoredPairs = scoredPairs_.filter(tuple => tuple._5 > burstConf.queryExpansionMatchThreshold)

      for ( tup <- scoredPairs.take(5) ) {
        println(tup._2)
      }

      val scoreList = scoredPairs.map(tuple => tuple._5).collect.sorted.reverse
      val topScoredPairs = if ( scoreList.length < burstConf.maxRelevantTweets ) {
        println("Match List: %d".format(scoreList.length))
        scoredPairs
      } else {
        val minScore = scoreList(burstConf.maxRelevantTweets-1)

        println("Match List is too big: %d".format(scoreList.length))
        println("\tUpdated minimum score: %f".format(minScore))

        scoredPairs.filter(tuple => tuple._5 >= minScore)
      }

//      val collectedMatches = scoredPairs.collect().sortBy(tuple => tuple._3).reverse
//
//      println("Tweets matching query [" + topic.title + "]:")
//      for ( tup <- collectedMatches ) {
//        println(tup._1 + "\t" + tup._2.replace("\n", " ") + "\t" + tup._3)
//      }

      val matchCount = scoredPairs.count()
      if ( matchCount < burstConf.queryExpMinTweetCount ) {
        println("Not enough tweets found for: " + topic.topid + ", " + matchCount)

        // Add the original topic back in
        newTopicList = newTopicList :+ topic
      } else {
        println("For topic [" + topic.topid + "], tweet count: " + matchCount)

        // Now build word2vec model from collected tweets, so we can extract synonyms
        val tokenizedTweets: RDD[(String, Int)] = topScoredPairs.flatMap(tuple => {
          tuple._3.filter(token => token.length >= burstConf.queryExpMinTokenSize).map(token => (token, 1))
        }).reduceByKey((l, r) => l + r)
        val foreModelTotal = tokenizedTweets.map(tuple => tuple._2).reduce((l, r) => l + r)
        val logForeTotal = math.log(foreModelTotal)
        val foreModel = tokenizedTweets.mapValues(count => math.log(count) - logForeTotal).collect()

        val topTokens = foreModel.map(tuple => {
          val token = tuple._1
          val fgProb = math.exp(tuple._2)
          val bgProb = math.exp(bgModel(token))

          val gain = fgProb * math.log(fgProb / bgProb)

          (token, gain)
        }).sortBy(tuple => tuple._2).reverse.take(burstConf.queryExpTokenTakeCount)

        println("Top keywords for: " + topic.topid)
        for ( pair <- topTokens ) {
          println("\t" + pair._1 + "->" + pair._2)
        }

        val topHashtags = scoredPairs.flatMap(tuple => tuple._4.map(hashtag => (hashtag, 1)))
          .reduceByKey((l, r) => l + r)
          .filter(tuple => tuple._2 > burstConf.queryExpMinHashtagCount)
          .sortBy(tuple => tuple._2, false)
          .take(burstConf.queryExpHashtagTakeCount)

        println("Top Hashtags for: " + topic.topid)
        for ( pair <- topHashtags ) {
          println("\t" + pair._1 + "->" + pair._2)
        }

        // New set of tokens
        val newTokenList : Array[String] = //Array[String](topic.query) ++
          topTokens.map(tuple => tuple._1) ++
          topHashtags.map(tuple => '#' + tuple._1)
        .filter(token => !stopWords.contains(token))

        val newTopic = Topic(topic.query, topic.topid, newTokenList.distinct.mkString(" OR "), topic.narrative)
        newTopicList = newTopicList :+ newTopic
      }
    }

    // Write topic list out
    val jsonData = Extraction.decompose(newTopicList)

    val file = new File(outputFile)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(pretty(jsonData))
    bw.close()
  }

  def buildBigrams(tokenList : List[String]) : List[String] = {
    return if ( tokenList.size > 1 ) {
      tokenList ++ tokenList.sliding(2).toList.map(x => x(0) + " " + x(1))
    } else {
      tokenList
    }
  }
}
