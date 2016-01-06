/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.umd.cs.hcil.twitter.spark.common

import java.util.Properties
import java.io.FileInputStream

@SerialVersionUID(1000L)
class Conf(propsFilePath : String) extends Serializable {

  val prop = new Properties()
  prop.load(new FileInputStream(propsFilePath))

  val minorWindowSize : Integer = prop.getProperty("MINOR_WINDOW_SIZE").toInt
  val majorWindowSize : Integer  = prop.getProperty("MAJOR_WINDOW_SIZE").toInt

  val perMinuteMax : Integer  = prop.getProperty("PER_MINUTE_MAX").toInt
  val burstThreshold : Double  = prop.getProperty("BURST_THRESHOLD").toDouble
  val similarityThreshold : Double  = prop.getProperty("SIM_THRESHOLD").toDouble

  val maxHashtags : Integer  = prop.getProperty("MAX_HASHTAGS").toInt
  val minTokens : Integer  = prop.getProperty("MIN_TOKENS").toInt
  val maxUrls : Integer  = prop.getProperty("MAX_URLS").toInt
//
//  def MINOR_WINDOW_SIZE = 2
//  def MAJOR_WINDOW_SIZE = 30 // Used 30 minutes here for TREC2015
//
//  def PER_MINUTE_MAX = 10
//  def BURST_THRESHOLD = 0.07
//  def SIM_THRESHOLD = 0.7
//
//  def MAX_HASHTAGS = 3
//  def MAX_URLS = 2
//  def MIN_TOKENS = 5
//


}
