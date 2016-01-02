/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.umd.cs.hcil.twitter.spark.common

object Conf {

  def MINOR_WINDOW_SIZE = 2
  def MAJOR_WINDOW_SIZE = 30 // Used 30 minutes here for TREC2015

  def PER_MINUTE_MAX = 10
  def BURST_THRESHOLD = 0.07
  def SIM_THRESHOLD = 0.7

  def MAX_HASHTAGS = 3
  def MAX_URLS = 2
  def MIN_TOKENS = 5

}
