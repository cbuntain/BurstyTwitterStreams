/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.umd.cs.hcil.twitter.spark.utils

import java.util.Calendar
import java.util.Date

object DateUtils {

  def convertTimeToSlice(time : Date) : Date = {
    val cal = Calendar.getInstance
    cal.setTime(time)
    cal.set(Calendar.SECOND, 0)
    
    return cal.getTime
  }

  def minorWindowDates(startDate : Date, minorWindowSize : Int) : List[Date] = {
    val cal = Calendar.getInstance
    cal.setTime(startDate)

    var l = List[Date]()

    for(i <- 1 to minorWindowSize) {
      l = l :+ cal.getTime
      cal.add(Calendar.MINUTE, 1)
    }

    return l
  }

  def constructDateList(startDate : Date, endDate : Date) : List[Date] = {
    val cal = Calendar.getInstance
    cal.setTime(startDate)

    var l = List[Date]()

    while(cal.getTime.before(endDate)) {
      l = l :+ cal.getTime
      cal.add(Calendar.MINUTE, 1)
    }
    l = l :+ endDate

    return l
  }

}
