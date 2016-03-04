package edu.umd.cs.hcil.twitter.spark.preparation

import org.apache.lucene.analysis.core.SimpleAnalyzer
import org.apache.lucene.index.memory.MemoryIndex
import org.apache.lucene.queryparser.simple.SimpleQueryParser

import java.io.FileWriter
import java.io.File
import java.io.BufferedWriter

import java.util.Date

import edu.umd.cs.hcil.twitter.spark.common.{Conf, ScoreGenerator}
import edu.umd.cs.hcil.twitter.spark.utils.DateUtils
import edu.umd.cs.twitter.tokenizer.TweetTokenizer
import org.apache.commons.csv.{CSVFormat, CSVPrinter}
import org.apache.spark._
import org.apache.spark.mllib.feature.Word2Vec
import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import twitter4j.{Status, TwitterObjectFactory, TwitterException}

import scala.collection.JavaConverters._
import scala.util.control.Breaks._

/**
 * Created by cbuntain on 2/25/16.
 */
object QueryExpander {

  implicit val formats = DefaultFormats // Brings in default date formats etc.
  case class Topic(title: String, num: String, tokens: List[String])



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

    val tweetTextTuples = tweets.map(status =>
      (status.getId,
        status.getText,
        tokenize(status.getText.toLowerCase).filter(token => {
          token.length >= burstConf.queryExpMinTokenSize
        }),
        status.getHashtagEntities.map(entity => entity.getText.toLowerCase)))

    val topicsJsonStr = scala.io.Source.fromFile(topicsFile).mkString
    val topicsJson = parse(topicsJsonStr)
    val topicList = topicsJson.extract[List[Topic]]

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
      val strippedTitle = topic.title.replace("\"", "")

      // Pseudo-Relevance feedback
      val scoredPairs = tweetTextTuples.mapPartitions(iter => {
        // Construct an analyzer for our tweet text
        val analyzer = new SimpleAnalyzer()
        val parser = new SimpleQueryParser(analyzer, "content")

        iter.map(pair => {
          val id = pair._1
          val text = pair._2
          val tokens = pair._3
          val hashtags = pair._4

          // Construct an in-memory index for the tweet data
          val idx = new MemoryIndex()

          idx.addField("content", text.toLowerCase(), analyzer)

          val score = idx.search(parser.parse(strippedTitle))

          (id, text, tokens, hashtags, score)
        })
      }).filter(tuple => tuple._5 > burstConf.queryExpansionMatchThreshold)

//      val collectedMatches = scoredPairs.collect().sortBy(tuple => tuple._3).reverse
//
//      println("Tweets matching query [" + topic.title + "]:")
//      for ( tup <- collectedMatches ) {
//        println(tup._1 + "\t" + tup._2.replace("\n", " ") + "\t" + tup._3)
//      }

      val matchCount = scoredPairs.count()
      if ( matchCount < 10 ) {
        println("Not enough tweets found for: " + topic.title + ", " + matchCount)

        // Add the original topic back in
        newTopicList = newTopicList :+ topic
      } else {
        println("For topic [" + topic.title + "], tweet count: " + matchCount)

        // Now build word2vec model from collected tweets, so we can extract synonyms
        val tokenizedTweets: RDD[(String, Int)] = scoredPairs.flatMap(tuple => {
          tuple._3.filter(s => s.length > 3).map(token => (token, 1))
        }).reduceByKey((l, r) => l + r)
        val foreModelTotal = tokenizedTweets.map(tuple => tuple._2).reduce((l, r) => l + r)
        val logForeTotal = math.log(foreModelTotal)
        val foreModel = tokenizedTweets.mapValues(count => math.log(count) - logForeTotal).collect()

//        val w2v = new Word2Vec
//        w2v.setMinCount(50)
//        w2v.setNumPartitions(1)
//        w2v.setVectorSize(100)
//        val model: Word2VecModel = w2v.fit(tokenizedTweets)
//
//        for (keyword <- topic.tokens) {
//          try {
//            val syns = model.findSynonyms(keyword, 5)
//
//            println("Synonyms for [" + keyword + "]:")
//            for (pair <- syns) {
//              println("\t" + pair._1 + "->" + pair._2)
//            }
//          } catch {
//            case e : Exception => println("Failed to find syn for keyword: " + keyword)
//          }
//        }

        val topTokens = foreModel.map(tuple => {
          val token = tuple._1
          val fgProb = math.exp(tuple._2)
          val bgProb = math.exp(bgModel(token))

          val gain = fgProb * math.log(fgProb / bgProb)

          (token, gain)
        }).sortBy(tuple => tuple._2).reverse.take(10)

        println("Top keywords for: " + topic.title)
        for ( pair <- topTokens ) {
          println("\t" + pair._1 + "->" + pair._2)
        }

        val topHashtags = scoredPairs.flatMap(tuple => tuple._4.map(hashtag => (hashtag, 1)))
          .reduceByKey((l, r) => l + r)
          .filter(tuple => tuple._2 > 1)
          .sortBy(tuple => tuple._2, false)
          .take(5)

        println("Top Hashtags for: " + topic.title)
        for ( pair <- topHashtags ) {
          println("\t" + pair._1 + "->" + pair._2)
        }

        // New set of tokens
        val newTokenList : List[String] = topic.tokens ++
          topTokens.map(tuple => tuple._1) ++
          topHashtags.map(tuple => '#' + tuple._1)

        val newTopic = Topic(topic.title, topic.num, newTokenList.distinct)
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

  // Split a piece of text into individual words.
  def tokenize(text : String) : Array[String] = {
    // Lowercase each word and remove punctuation.
    text.toLowerCase.replaceAll("[^a-zA-Z0-9\\s]", "").split("\\s+")
  }
}
