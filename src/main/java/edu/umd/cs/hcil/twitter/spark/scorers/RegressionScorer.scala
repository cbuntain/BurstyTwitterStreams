/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.umd.cs.hcil.twitter.spark.scorers

import org.apache.commons.math3.stat.regression.SimpleRegression

object RegressionScorer extends Scorer {
  
  var mFreqSums : Array[Long] = Array.empty
  var mSmoother : Double = 0
  
  def score(freq : Array[Long]) : Double = {
    val regress = new SimpleRegression(true)
    for ( i <- 0 until freq.length ) {
      val thisWindowSum = mFreqSums(i)
      val smoothedFreqPercent = (freq(i) + 1) / (thisWindowSum + mSmoother)
      val logVal = scala.math.log(smoothedFreqPercent)
      
      regress.addData(i, logVal)
    }
    
    return (regress.getSlope * regress.getRSquare)
  }
  
}
