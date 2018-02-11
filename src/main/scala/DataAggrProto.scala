/**
  * Created by Dheefinity on 1/3/2018.
  */

package com.dhee

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.streaming
import org.apache.spark.sql.streaming.Trigger
///import org.apache.spark.sql.types.TimestampType
import java.sql.Timestamp

case class DealPnlData(busDate: String, dealId: String,
                       prodId: String, portFolioId: String, scenarioId: Int, pnl: Float,
                       baseOrWhatIf : String,
                       uid: String, eventTime: Timestamp)

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
  .selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
  .as[(String, Long)]

   val rawDataDF = //lines.map(_.split(","))
                    lines.map(elt => elt._1.split(",")  ++ Array(elt._2.toString))
                          .map(attr => DealPnlData(attr(0), attr(1),attr(2)
                                                    ,attr(3),attr(4).trim.toInt,attr(5).trim.toFloat
                                                    , attr(6)
                              ,attr(0)+attr(1)+attr(2)+attr(3)+attr(4)+attr(6), new Timestamp(attr(7).toLong)     ) )
                                .withWatermark("eventTime", "1 minute")
                            .dropDuplicates("uid").toDF()

    val parquetQuery = rawDataDF.writeStream
      .format("parquet")        // can be "orc", "json", "csv", etc.
      .option("checkpointLocation", "/home/kumar/DataAggregation/baseDfData")
      .option("path", "/home/kumar/DataAggregation/baseDfData")
      .outputMode("append")
      .start()

    rawDataDF.createOrReplaceTempView("pnlStructTable")
    val baseAggDf = spark.sql(
      s"""select portfolioId, scenarioId, sum(pnl)
         |from pnlStructTable WHERE baseOrWhatif = 'BASE' group by portfolioId, scenarioId""".stripMargin)


    val baseWriter = new JDBCSink(url, user, pwd, "base_aggr")

    val query = baseAggDf
      .writeStream
      .foreach(baseWriter)
      //.trigger(Trigger.ProcessingTime("30 seconds"))
      .outputMode("update") // could also be append or update
      .start()



    //val whatIfAggDf = spark.sql("select portfolioId, scenarioId, sum(pnl)
    // from pnlStructTable WHERE baseOrWhatif = 'WHATIF' group by portfolioId, scenarioId")
    //val whatIfWriter = new JDBCSink(url, user, pwd, "whatif_aggr")

//    val distinctWhatIfPortfolios = spark.sql(
//                """Select distinct portfolioid
//                    |from pnlStructTable where baseOrWhatif = 'WHATIF'""".stripMargin)
//    distinctWhatIfPortfolios.createOrReplaceTempView("distinctPnlsTable")
//
//    val whatIfAggBaseDf = spark.sql(
//      s"""select portfolioId, scenarioId, sum(pnl) from pnlStructTable
//         |  WHERE baseOrWhatif = 'BASE'
//         |  and portfolioid not in (Select distinct portfolioid from distinctPnlsTable)
//         |  group by portfolioId, scenarioId
//         |  """.stripMargin)

//    val whatIfAggOnlyWhatIfDf = spark.sql(
//      s"""select portfolioId, scenarioId, sum(pnl) from pnlStructTable
//         |  WHERE baseOrWhatif = 'WHATIF'
//         |  group by portfolioId, scenarioId
//         |  """.stripMargin)

//    val query_WhatIfBase = whatIfAggBaseDf
//      .writeStream
//      .foreach(whatIfWriter)
//      //.trigger(Trigger.ProcessingTime("30 seconds"))
//      .outputMode("update") // could also be append or update
//      .start()

//    val query_WhatIfWhatIf = whatIfAggOnlyWhatIfDf
//      .writeStream
//      .foreach(whatIfWriter)
//      //.trigger(Trigger.ProcessingTime("30 seconds"))
//      .outputMode("update") // could also be append or update
//      .start()

  query.awaitTermination()
   // query_WhatIfBase.awaitTermination()
   // query_WhatIfWhatIf.awaitTermination()

    parquetQuery.awaitTermination()
  }
}
