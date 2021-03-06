/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.umd.cs.hcil.twitter.spark.learners

import java.util.Date
import java.util.Locale
import java.text.SimpleDateFormat

import scala.collection.JavaConverters._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import twitter4j.Status
import twitter4j.json.DataObjectFactory
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVPrinter
import edu.umd.cs.hcil.twitter.spark.common.Conf
import edu.umd.cs.hcil.twitter.spark.common.ScoreGenerator
import edu.umd.cs.hcil.twitter.spark.utils.{DateUtils, StatusTokenizer}

object LearnerApp {
  
  // Twitter's time format'
  def TIME_FORMAT = "EEE MMM d HH:mm:ss Z yyyy"
  
  class TokenizedStatus(val s : Status, val t : List[String]) {
    val status : Status = s
    val tokens : List[String] = t
  }

  /**
   * @param args the command line arguments
   */
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("TwitterLearner")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    val propertiesPath = args(0)
    val burstConf = new Conf(propertiesPath)

    val dataPath = args(1)
    val outputPath = args(2)
    
    val twitterMsgsRaw = sc.textFile(dataPath)
    println("Initial Partition Count: " + twitterMsgsRaw.partitions.size)
    
    var twitterMsgs = twitterMsgsRaw
    if ( args.size > 3 ) {
      val initialPartitions = args(3).toInt
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
          DataObjectFactory.createStatus(line)
        } catch {
          case e : Exception => null
        }
      })
    
    // Only keep non-null status with text, and remove all retweets
    val tweetsFiltered = tweets.filter(status => {
        status != null &&
        status.getText != null &&
        status.getText.size > 0 &&
        status.isRetweet == false
      })
    
    // Create a keyed pair with Date -> Tweet Map
    val timedTweets = tweetsFiltered.map(tweet => 
      (DateUtils.convertTimeToSlice(tweet.getCreatedAt), List(tweet)))

    // Combine same Date keys to create a list of tweets for that date
    val groupedTweets : RDD[Tuple2[Date, List[Status]]] = timedTweets.reduceByKey(
      (l, r) => l ++ r
    )

    // Extract the dates in this RDD
    val times = groupedTweets.keys

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
    var fullKeyList = DateUtils.constructDateList(minTime, maxTime)
    println("Date Key List Size: " + fullKeyList.size)

    val fullKeyTupleList : List[Tuple2[Date, List[Status]]] = 
      fullKeyList.map(key => (key, List()))
    val fullKeyRdd : RDD[Tuple2[Date, List[Status]]] = 
      sc.parallelize(fullKeyTupleList, newPartitionSize)

    // Merge the full date RDD with the existing data to fill any gaps
    val withFullDates = groupedTweets.union(fullKeyRdd)
    val mergedDates = withFullDates.reduceByKey((l, r) => l ++ r, newPartitionSize)
    printf("mergedDates Partition Size: " + mergedDates.partitions.size + "\n")
    
    // Tokenize all the tweets. 
    //  NOTE: This could take a while...
    val tokenizedTweets = mergedDates.map(tuple => {
        val date = tuple._1
        val statusList = tuple._2
        
        val tokenizedStatuses = statusList.map(status => {
            try {

              val hashtagList: List[String] = status.getHashtagEntities.toList.map(hte => "#" + hte.getText)
              val tokens: List[String] = StatusTokenizer.tokenize(status) ++ hashtagList

              new TokenizedStatus(status, tokens)
            } catch {
              case e : Exception => null
            }
          }).filter(status => status != null)
        
        (date, tokenizedStatuses)
      })
    
    // Create a map and invert map for our date list, so we can merge dates 
    //  for the minor window
    val windows = mergeToMinorWindows(tokenizedTweets, fullKeyList, burstConf.minorWindowSize)
    printf("windows Partition Size: " + windows.partitions.size + "\n")
    
    // Create an RDD of dates to user token counts
    val datedUserFrequencies = frequencyUser(windows)
    datedUserFrequencies.persist
    
    // Slide over the date list using our major window size
    var scoreData : RDD[Tuple3[Date, String, Double]] = sc.parallelize(List.empty)
    for (dateWindow <- fullKeyList.sliding(burstConf.majorWindowSize)) {
      
      // Only keep those dates that are in this window
      val thisWindowRdd = datedUserFrequencies.filter(tuple => {
          val date = tuple._1
          dateWindow.contains(date)
        })
      
      // Invert the Date->String->Int map to String->Date->Int
      val invertedRdd = thisWindowRdd.flatMap(tuple => {
          val date = tuple._1
          val tokenMap = tuple._2
          
          tokenMap.mapValues(count => Map(date -> count))
        }).reduceByKey((l, r) => l ++ r)
      
      // TODO: Remove debugging statements
      println("Running Window: " + dateWindow)  
      
      val scores = ScoreGenerator.scoreFrequencyArray(invertedRdd, dateWindow)
      val timedScores = scores.map(keywordScore => (dateWindow.last, keywordScore._1, keywordScore._2))
      scoreData = scoreData ++ timedScores

      // Debugging print statements
//      val sortedScores = scores.sortBy(tuple => tuple._2, false)
//      val topList = sortedScores.take(50)
//      println("\nPopular topics, Now: %s, Window: %s".format(new Date().toString, dateWindow.last.toString))
//      topList.foreach{case (tag, score) => println("%s - %f)".format(tag, score))}
    }
    
    val consolidated = scoreData.repartition(newPartitionSize).sortBy(tuple => tuple._1)
    consolidated.map(tuple => {
        val buff = new StringBuffer()
        val writer = new CSVPrinter(buff, CSVFormat.DEFAULT)
        
        val sdf = new SimpleDateFormat(TIME_FORMAT, Locale.US);
        sdf.setTimeZone(java.util.TimeZone.getTimeZone("UTC"));

        writer.print(sdf.format(tuple._1))
        writer.print(tuple._2)
        writer.print(tuple._3)
        
        buff.toString
      }).saveAsTextFile(outputPath)
  }
  
  /**
   * This function will take an RDD of Date->Tweets maps and merge them such that
   *  the previous windowSize-1 dates are combined with the current date. As a result,
   *  we truncate off the first windowSize-1 dates
   */
  def mergeToMinorWindows(
    dataFrame : RDD[Tuple2[Date,List[TokenizedStatus]]],
    dateList : List[Date],
    windowSize : Int
  ) : RDD[Tuple2[Date,List[TokenizedStatus]]] = {
    
    val numDates : Int = dateList.size
    val dateIndexMap : Map[Date, Int] = dateList.zip(0 to numDates-1).toMap

    var windowList = dataFrame
    for ( i <- 0 to windowSize-2 ) {
      val offset = windowSize - i - 1
      val slidDates = dataFrame.map(dateTuple => {
          val date = dateTuple._1
          val tweetList = dateTuple._2
          
          if ( dateIndexMap.contains(date) == true ) {
            val newDateIndex = dateIndexMap(date) + offset
            if ( newDateIndex >= numDates ) {
              null
            } else {
              val newDate = dateList(newDateIndex)
              (newDate, tweetList)
            }
          } else {
            null
          }
        })

      val slidDatesFiltered = slidDates.filter(x => x != null)
      windowList = windowList.leftOuterJoin(slidDatesFiltered).combineByKey(
        (value : (List[TokenizedStatus], Option[List[TokenizedStatus]])) => {
          val left = value._1
          val right = value._2
          
          right match {
            case Some(x) => left ++ x
            case None => left
          }
        },
        (x : List[TokenizedStatus], value : (List[TokenizedStatus], Option[List[TokenizedStatus]])) => {
          x ++ value._1 ++ value._2
          
          val left = value._1
          val right = value._2
          
          right match {
            case Some(y) => x ++ left ++ y
            case None => x ++ left
          }
        },
        (x: List[TokenizedStatus], y: List[TokenizedStatus]) => { 
          x ++ y
      })
    }
    
    return windowList
  }
  
  /**
   * This function takes an RDD map of Date->List[Tweet] and counts the number
   *  of users using each token in a given date
   */
  def frequencyUser(
    windows : RDD[Tuple2[Date,List[TokenizedStatus]]]
  ) : RDD[Tuple2[Date, Map[String, Int]]] = {
    
    val userCounts = windows.mapValues(statusList => {
        val userToTokens = statusList
          .map(status => (status.status.getId, status.tokens))
          .groupBy(tuple => tuple._1)
          .mapValues(list => list.map(tuple => tuple._2).reduce((l, r) => l ++ r))
          .mapValues(list => list.toSet)
          
        val tokenCounts = userToTokens
          .flatMap(tuple => tuple._2)
          .map(token => (token, 1))
          .groupBy(tuple => tuple._1)
          .mapValues(list => list.map(tuple => tuple._2))
          .mapValues(list => list.reduce((l, r) => l + r))
          
        tokenCounts
      })
    
    return userCounts
  }
  
}
