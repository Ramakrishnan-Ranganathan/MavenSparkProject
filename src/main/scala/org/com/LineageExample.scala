package org.com

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object LineageExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Test")
      .master("local[*]")
      .getOrCreate()

//    val lines = spark.sparkContext.textFile("src/main/resources/input.txt")
//    val words = lines.flatMap(line => line.split(" "))
//    val wordCounts = words.map(word => (word, 1)).reduceByKey(_ + _)
//    wordCounts.collect()
//    println(wordCounts.toDebugString)

    val df = spark.read.format("csv").load("src/main/resources/Customer.csv")
    df.show(false)



  }

}
