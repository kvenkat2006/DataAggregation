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

//RanSud: Typesafe changes next 1 line
import com.typesafe.config.ConfigFactory

case class DealPnlData(busDate: String, dealId: String,
                       prodId: String, portFolioId: String, scenarioId: Int, pnl: Float,
                       baseOrWhatIf : String,
                       uid: String, eventTime: Timestamp)

object DataAggrProto {
  def main(args: Array[String]) {

  var logger = Logger.getLogger(this.getClass())

  val jobName = "DataAggrProto"

//RanSud: Typesafe changes next 13 lines
  val config = ConfigFactory.parseFile(new java.io.File("src/main/scala/dataagg.conf"))

//val user="postgres"
  val user = config.getString("dataagg.config.general.pgresuser")
//val pwd="postgres"
  val pwd = config.getString("dataagg.config.general.pgrespw") 
//val db="DATA_AGGR"
  val db = config.getString("dataagg.config.consumer.dbname")
  val tbl = config.getString("dataagg.config.consumer.tblname")
  val topic = config.getString("dataagg.config.general.kafkatopic")
  val broker = config.getString("dataagg.config.general.kafkabroker")
  val baseDFpath = config.getString("dataagg.config.consumer.baseDFloc")

//val url = s"""jdbc:postgresql://localhost:5432/DATA_AGGR?user=postgres&password=postgres"""
  val url = s"""jdbc:postgresql://localhost:5432/$db?user=$user&password=$pwd"""

   val spark = SparkSession.builder.
  master("local[2]")
  .appName("spark session example")
  .getOrCreate()

  import spark.implicits._
  val lines = spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", broker)  //RanSud: typesafe changes, replaced with broker
  .option("subscribe", topic)                 //RanSud: typesafe changes, replaced with topic
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
      .option("checkpointLocation", baseDFpath)   //RanSud: typesafe changes, replaced with baseDFpath
      .option("path", baseDFpath)                 //RanSud: typesafe changes, replaced with baseDFpath
      .outputMode("append")
      .start()

    rawDataDF.createOrReplaceTempView("pnlStructTable")
    val baseAggDf = spark.sql(
      s"""select portfolioId, scenarioId, sum(pnl)
         |from pnlStructTable WHERE baseOrWhatif = 'BASE' group by portfolioId, scenarioId""".stripMargin)

    val baseWriter = new JDBCSink(url, user, pwd, tbl)  //RanSud: typesafe changes, replaced with tbl

    val query = baseAggDf
      .writeStream
      .foreach(baseWriter)
      //.trigger(Trigger.ProcessingTime("30 seconds"))
      .outputMode("update") // could also be append or update
      .start()

    query.awaitTermination()

    parquetQuery.awaitTermination()
  }
}
