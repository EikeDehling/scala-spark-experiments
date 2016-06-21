package com.examples

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import play.api.libs.json.JsObject
import play.api.libs.json.JsString
import play.api.libs.json.Json

object TwitterTrending {
  
  val parser = FastDateFormat.getInstance("EEE MMM d HH:mm:ss Z yyyy")
  val formatter = FastDateFormat.getInstance("yyyy-MM-dd")
  
  def main(arg: Array[String]) {
    val sc = new SparkContext(new SparkConf().setMaster("local[3]").setAppName("TwitterTrending")) 

    val input = sc.textFile("twitter-sample.json")

    val rows = input
                    // Json decode all the tweets
                    .map(Json.parse(_))
                    
                    // Remove any non-tweet entries (new followers, deletions, etc)
                    .filter(x => x != null &&
                                 x.as[JsObject].keys.contains("text") &&
                                 x.as[JsObject].keys.contains("created_at"))
                                 
                    // Convert to ((day, word), count) tuples
                    .flatMap(x => {
                                 val day = formatter.format(parser.parse((x\"created_at").as[JsString].value))
                                 for {
                                     word <- (x\"text").as[JsString].value.split("[ ,.;:@#()/!?]")
                                     if word.length() >= 3
                                 } yield ((day, word), 1)
                             })
                    
                    // Count word occurences per day
                    .reduceByKey(_ + _)
                    
                    // Change to (day, (word, count)) tuples
                    .map(x => (x._1._2, (x._1._1, x._2)))
                    
                    // List of word-counts per day
                    .groupByKey()
                    
                    // Calculate score of the words on each day
                    .flatMap(x => {
                                 // Score by slope (increase compared to previous measurement)
                                 // Common words, such as the/and/by/me, are scored high by this ; results are not very interesting
                                 /* val shifted = (null, 0) +: x._2.toList
                                 for (((date, count), (ignored, previous_count)) <- x._2 zip shifted) yield {
                                     (date, (x._1, count - previous_count))
                                 } */
                                 
                                 // Score by absolute/relative increase in frequency
                                 // Relative increase in frequency returns uncommon words, such as usernames or misspellings
                                 // Absolute increase in frequency returns frequent words, such as stop words
                                 val mean = x._2.map(_._2).sum / x._2.size
                                 for ((date, count) <- x._2) yield {
                                     //(date, (x._1, count - mean)) // Score by absolute increase in frequency
                                     (date, (x._1, count / mean)) // Score by relative increase in frequency
                                 }
                             })
                             
                    // Group and sort the results
                    .groupByKey()
                    .map(x => (x._1, x._2.toList.sortBy(- _._2).take(5)))
                    .collect()

    Logger.getLogger(this.getClass).info("=> topN" + rows.mkString(", "))
  }
}