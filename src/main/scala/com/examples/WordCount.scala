package com.examples

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger

object WordCount {
  def main(arg: Array[String]) {
    var logger = Logger.getLogger(this.getClass())

    val conf = new SparkConf().setMaster("local[2]").set("spark.executor.memory","1g").setAppName("WordCount")
    val sc = new SparkContext(conf) 

    val input = sc.textFile("input.txt")

    val rows = input.flatMap(_.split(" "))
                    .map((_, 1))
                    .reduceByKey(_ + _)
                    .map(_.swap)
                    .sortByKey(false)

    logger.info("=> topN" + rows.take(5).mkString(", "))
  }
}
