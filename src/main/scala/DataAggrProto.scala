/**
  * Created by Kumar on 1/3/2018.
  */

package com.dhee

//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.SparkSession

//import org.apache.spark.SparkContext
//import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object DataAggrProto {
  def main(args: Array[String]) {

  var logger = Logger.getLogger(this.getClass())

  val jobName = "DataAggrProto"

  val spark = SparkSession.builder.
  master("local[2]")
  .appName("spark session example")
  .getOrCreate()

  import spark.implicits._
  val lines = spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9092")
  .option("subscribe", "test")
  .option("startingOffsets", "earliest")
  .load()
  //.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)")
  .selectExpr("CAST(value AS STRING)")
  //.as[(String, String)]
  .as[String]

  // Generate running word count
  val wordCounts = lines.flatMap(_.split(" ")).groupBy("value").count()

  // Start running the query that prints the running counts to the console
  //val query = wordCounts.writeStream
  //.outputMode("complete")
  //.format("console")
  //.start()

  //val wordCounts = lines.groupBy("key").count()
  val query = wordCounts.writeStream
  .outputMode("complete")
  .format("console")
  .start()

  query.awaitTermination()

  }
}
