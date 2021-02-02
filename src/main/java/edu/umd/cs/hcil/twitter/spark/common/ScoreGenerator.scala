/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.umd.cs.hcil.twitter.spark.common

import java.util.Date
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.commons.math3.linear.ArrayRealVector
import edu.umd.cs.hcil.twitter.spark.scorers.RegressionScorer

object ScoreGenerator {

  def scoreUndatedList(memory : Int, combinedRdd : RDD[(String, List[Int])]) : RDD[(String, Double)] = {

    // Count up all the tokens in this window
    val smoothFactor = combinedRdd.keys.count

    // Calculate the sum at each time slice
    val sliceSums = combinedRdd.aggregate(
      Array.fill(memory)(0L)
    )((sumArr, value) => {
      val accum = Array.fill(memory)(0L)

      val thisDimSum = sumArr
      val thisTokenDim = value._2

      val untilIndex = Math.min(memory, thisTokenDim.length)

      for ( j <- 0 until untilIndex ) {
        val currentSum = thisDimSum(j)
        val thisTokenCount = thisTokenDim(j)

        accum(j) = currentSum + thisTokenCount
      }

      accum

    }, (sumArr, sumArr2) => {
      val accum = Array.fill(memory)(0L)

      for ( j <- 0 until memory ) {
        accum(j) = sumArr(j) + sumArr2(j)
      }

      accum
    })

    // Now score the frequency arrays
    val featureRdd = combinedRdd.mapValues(v => {

      // Make sure the regression scorer knows the normalizing factors
      RegressionScorer.mFreqSums = sliceSums
      RegressionScorer.mSmoother = smoothFactor

      val arrRaw = v.map(i => i.toLong).toArray.takeRight(memory)

      Array(
        RegressionScorer.score(arrRaw)
      )
    })

    val normalizedFeatures = featureRdd

    val normRdd = normalizedFeatures.mapValues(arr => {
      new ArrayRealVector(arr).getNorm
    })

    return normRdd
  }

  def scoreFrequencyArray(combinedRdd : RDD[(String, Map[Date, Int])], dateList : List[Date]) : RDD[(String, Double)] = {
    
    // Make sure each token has an entry for each date
    val allDatesCombinedRdd = combinedRdd.mapValues(m => {
        var fullMap : Map[Date, Int] = m

        for (d <- dateList) {
          if ( m.contains(d) == false ) {
            fullMap = fullMap + (d -> 0)
          }
        }

        fullMap
      })
    
    // Count up all the tokens in this window
    val smoothFactor = allDatesCombinedRdd.keys.count

    // Construct a map from strings to frequency arrays. 
    //  Each string maps to an array containing the 
    //  token's frequency across the window
    val frequencyRdd = allDatesCombinedRdd.mapValues(valueMap => {
        val sortedKeys = dateList

        var listRaw : List[Long] = List.empty

        for ( k <- sortedKeys ) {
          listRaw = listRaw :+ valueMap(k).asInstanceOf[Long]
        }

        val arrRaw = listRaw.toArray

        arrRaw
      })

    // Calculate the sum at each time slice
    val sliceSums = frequencyRdd.aggregate(
      Array.fill(dateList.size)(0L)
    )((sumArr, value) => {
      val accum = Array.fill(dateList.size)(0L)

      val thisDimSum = sumArr
      val thisTokenDim = value._2

      for ( j <- 0 to thisDimSum.length - 1 ) {
        val currentSum = thisDimSum(j)
        val thisTokenCount = thisTokenDim(j)

        accum(j) = currentSum + thisTokenCount
      }

      accum

    }, (sumArr, sumArr2) => {
      val accum = Array.fill(dateList.size)(0L)

      for ( j <- 0 to sumArr.length - 1 ) {
        accum(j) = sumArr(j) + sumArr2(j)
      }

      accum
    })
    
    // Now score the frequency arrays
    val featureRdd = frequencyRdd.mapValues(v => {
        
        // Make sure the regression scorer knows the normalizing factors
        RegressionScorer.mFreqSums = sliceSums
        RegressionScorer.mSmoother = smoothFactor
        
        val arrRaw = v

        Array(
          RegressionScorer.score(arrRaw)
          )
      })

    // Find the min and max scores for each feature, so we can normalize
//    val featureCount = 1
//    val scoreBounds = featureRdd.aggregate(
//      (Array.fill(featureCount)(Double.MaxValue),
//       Array.fill(featureCount)(Double.MinValue))
//    )((u, v) => {
//        var minArr = Array.fill(featureCount)(Double.MaxValue)
//        var maxArr = Array.fill(featureCount)(Double.MinValue)
//
//        val valueArray = v._2
//        for ( i <- 0 to valueArray.length - 1) {
//          val score = valueArray(i)
//          val thisMin = u._1(i)
//          val thisMax = u._2(i)
//
//          minArr(i) = math.min(score, thisMin)
//          maxArr(i) = math.max(score, thisMax)
//        }
//
//        (minArr, maxArr)
//      },
//      (u1, u2) => {
//        var minArr1 = u1._1
//        var maxArr1 = u1._2
//        var minArr2 = u2._1
//        var maxArr2 = u2._2
//
//        var minArr = Array.fill(featureCount)(Double.MaxValue)
//        var maxArr = Array.fill(featureCount)(Double.MinValue)
//
//        for ( i <- 0 to minArr1.length - 1) {
//          minArr(i) = math.min(minArr1(i), minArr2(i))
//          maxArr(i) = math.max(maxArr1(i), maxArr2(i))
//        }
//
//        (minArr, maxArr)
//      })
//
//    val minScores = scoreBounds._1
//    val maxScores = scoreBounds._2

//    println(dateList.last)
//    println("\tMin: " + minScores.toList.toString)
//    println("\tMax: " + maxScores.toList.toString)

    // TODO: Remove or check this normalization step. Do we do better or worse with it?
//    val normalizedFeatures = featureRdd.mapValues(featureArray => {
//        val normalizedArray : Array[Double] = Array.fill(featureArray.length)(0)
//
//        for ( i <- 0 to featureArray.length - 1 ) { 
//          normalizedArray(i) = (featureArray(i) - minScores(i)) / (maxScores(i) - minScores(i))
//        }
//
//        normalizedArray
//      })
    val normalizedFeatures = featureRdd
    
    val normRdd = normalizedFeatures.mapValues(arr => {
        new ArrayRealVector(arr).getNorm
      })
    
    return normRdd
  }
}
