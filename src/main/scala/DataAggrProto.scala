/**
  * Created by Kumar on 1/3/2018.
  */

package com.dhee

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, SparkSession}

case class DealPnlData(busDate: String, dealId: String, prodId: String, portFolioId: String, scenarioId: Int, pnl: Float)

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
 // val wordCounts = lines.flatMap(_.split(" ")).groupBy("value").count()
  //val wordCounts = lines.map(_.split(",")).filter(attr => attr(4) == 1).map(attr => (attr(3), attr(5)) ).groupByKey
//.groupBy("value").count()

//case class DealPnlData(busDate: String, dealId: String, prodId: String, portFolioId: String, scenarioId: Int, pnl: Float)

   val wordCounts = lines.map(_.split(","))
                          .map(attr => DealPnlData(attr(0), attr(1),attr(2),attr(3),attr(4).trim.toInt,attr(5).trim.toFloat ) ).toDF()

    //val ds: Dataset[DealPnlData] = wordCounts.as[DealPnlData]

    wordCounts.createOrReplaceTempView("pnlStructTable")
    val aggDf = spark.sql("select portfolioId, sum(pnl) from pnlStructTable group by portfolioId")
    // Start running the query that prints the running counts to the console
  //val query = wordCounts.writeStream
  //.outputMode("complete")
  //.format("console")
  //.start()

  //val wordCounts = lines.groupBy("key").count()
//  val query = wordCounts.writeStream
//  .outputMode("complete")
//  .format("console")
//  .start()

    val query = aggDf.writeStream.outputMode("complete").format("console").start()

  query.awaitTermination()

  }
}
