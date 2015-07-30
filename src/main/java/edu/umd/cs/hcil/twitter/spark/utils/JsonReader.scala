package edu.umd.cs.hcil.twitter.spark.utils

import org.json4s._
import org.json4s.jackson.JsonMethods._

object JsonReader {

  implicit val formats = DefaultFormats // Brings in default date formats etc.
  case class Topic(title: String, num: String, tokens: List[String])

  def main(args: Array[String]): Unit = {

    val fileName = args(0)

    val jsonStr = scala.io.Source.fromFile(fileName).mkString

    println(jsonStr)

    val json = parse(jsonStr)
    println(json)

    val topicList = json.extract[List[Topic]]

    for ( t <- topicList ) {
      println(t)
    }
  }

}