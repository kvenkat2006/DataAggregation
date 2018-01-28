/**
  * Created by Dheefinity on 1/3/2018.
  */

package com.dhee

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.streaming
import org.apache.spark.sql.streaming.Trigger

case class DealPnlData(busDate: String, dealId: String,
                       prodId: String, portFolioId: String, scenarioId: Int, pnl: Float,
                       baseOrWhatIf : String)

object DataAggrProto {
  def main(args: Array[String]) {

  var logger = Logger.getLogger(this.getClass())

  val jobName = "DataAggrProto"

    val user="postgres"
    val pwd="postgres"
    val db="DATA_AGGR"
    val url = s"""jdbc:postgresql://localhost:5432/DATA_AGGR?user=postgres&password=postgres"""

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
  .selectExpr("CAST(value AS STRING)")
  .as[String]

   val rawDataDF = lines.map(_.split(","))
                          .map(attr => DealPnlData(attr(0), attr(1),attr(2),attr(3),attr(4).trim.toInt,attr(5).trim.toFloat, attr(6) ) ).toDF()


    rawDataDF.createOrReplaceTempView("pnlStructTable")
    val baseAggDf = spark.sql("select portfolioId, scenarioId, sum(pnl) from pnlStructTable WHERE baseOrWhatif = 'BASE' group by portfolioId, scenarioId")


    val baseWriter = new JDBCSink(url, user, pwd, "base_aggr")

    val query = baseAggDf
      .writeStream
      .foreach(baseWriter)
      //.trigger(Trigger.ProcessingTime("30 seconds"))
      .outputMode("update") // could also be append or update
      .start()

    val whatIfAggDf = spark.sql("select portfolioId, scenarioId, sum(pnl) from pnlStructTable WHERE baseOrWhatif = 'WHATIF' group by portfolioId, scenarioId")
    val whatIfWriter = new JDBCSink(url, user, pwd, "whatif_aggr")

    val query_WhatIf = whatIfAggDf
      .writeStream
      .foreach(whatIfWriter)
      //.trigger(Trigger.ProcessingTime("30 seconds"))
      .outputMode("update") // could also be append or update
      .start()

  query.awaitTermination()
  query_WhatIf.awaitTermination()

  }
}
