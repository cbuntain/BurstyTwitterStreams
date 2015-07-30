/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.umd.cs.hcil.twitter.spark.scorers

object AverageScorer extends Scorer {
  
  def score(arr : Array[Long]) : Double = {
    
    var sum = 0L
    for ( i <- arr ) {
      sum += i
    }
    val avg = sum / (arr.length).asInstanceOf[Double]
    val score : Double = arr(arr.length - 1) - avg
    
    return score
  }
  
}
