package com.dhee
//import org.apache.spark.sql.expressions.Aggregator
//import org.apache.spark.sql.Encoder
//import org.apache.spark.sql.Encoders
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
//RanSud: Typesafe changes next 1 line
import com.typesafe.config.ConfigFactory
object StaticDataAggrProto {
  def main(args: Array[String]): Unit = {
//RanSud:Typesafe changes next 1 line
    val config = ConfigFactory.parseFile(new java.io.File("src/main/scala/dataagg.conf"))
    val url = "jdbc:postgresql://localhost:5432/DATA_AGGR"
    val prop = new java.util.Properties
//Sudha: Typesafe change 2 lines
    val usr = config.getString("dataagg.config.general.pgresuser")
    val pwd = config.getString("dataagg.config.general.pgrespw")
    prop.setProperty("driver", "org.postgresql.Driver")
//Sudha:Typesafe change 4 lines
    prop.setProperty("user", usr)
    prop.setProperty("password", pwd)
    //prop.setProperty("user", "developer")
    //prop.setProperty("password", "developer")
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("Spark SQL basic example")
      //.config("spark.some.config.option", "xxx")
      .getOrCreate()

//Sudha: Typesafe change 3 line
        val basedfpath = config.getString("dataagg.config.consumer.baseDFloc")
        val rawDataDF = spark.read.load(s"""$basedfpath/*.parquet""")
      // val rawDataDF = spark.read.load("/home/kumar/DataAggregation/baseDfData/*.parquet")
   rawDataDF.createOrReplaceTempView("pnlStructTable")
    rawDataDF.printSchema()
    println("\n\n ******* Count of elements in rawDataDF : " + rawDataDF.count())
    val baseAggDf = spark.sql(
      s"""select portfolioId as portfolio_id, scenarioId as scenario_id, sum(pnl) as sum_pnl
         |from pnlStructTable WHERE baseOrWhatif = 'BASE' group by portfolioId, scenarioId""".stripMargin)
    val rawBaseSubsetDF = spark.sql(
      s"""select busDate, dealId, prodId, portFolioId, scenarioId, pnl, baseOrWhatIf, uid, eventTime
         | from pnlStructTable WHERE baseOrWhatif = 'BASE' """.stripMargin)
    println("\n\n ******* Count of elements in rawBaseSubsetDF : " + rawBaseSubsetDF.count())
    val rawWhatIfSubsetDF = spark.sql(

    s"""select busDate, dealId, prodId, portFolioId, scenarioId, pnl as whatIfPnl, baseOrWhatIf, uid, eventTime
         | from pnlStructTable WHERE baseOrWhatif = 'WHATIF' """.stripMargin)
    println("\n\n ******* Count of elements in rawWhatIfSubsetDF : " + rawWhatIfSubsetDF.count())
//    val testDF = rawBaseSubsetDF.join(rawWhatIfSubsetDF,
//                    Seq("busDate", "dealId", "prodId", "portFolioId", "scenarioId"),
//                    "inner")
//
//    testDF.show()
    val rawBaseJoinWhatIfDF = rawBaseSubsetDF.join(rawWhatIfSubsetDF,
                                            Seq("busDate", "dealId", "prodId", "portFolioId", "scenarioId"),
                                                "left_outer")
    println("\n\n ******* Count of elements in rawBaseJoinWhatIfDF : " + rawBaseJoinWhatIfDF.count())
    //rawBaseJoinWhatIfDF
    val whatIfFusedDF = rawBaseJoinWhatIfDF.select(rawBaseSubsetDF("busDate"),
                          rawBaseSubsetDF("dealId"),
                          rawBaseSubsetDF("prodId"),
rawBaseSubsetDF("portFolioId"),
                          rawBaseSubsetDF("scenarioId"),
                          when(rawWhatIfSubsetDF("whatIfPnl").isNotNull,rawWhatIfSubsetDF("whatIfPnl"))
                                    .otherwise(rawBaseSubsetDF("pnl")).as("pnl")
                          )
    whatIfFusedDF.createOrReplaceTempView("whatIfFusedTable")
    val whatIfAggDf = spark.sql(
      s"""select portfolioId as portfolio_id, scenarioId as scenario_id, sum(pnl) as sum_pnl
         |from whatIfFusedTable group by portfolioId, scenarioId""".stripMargin)
    whatIfAggDf.write.mode("overwrite").jdbc(url, "whatif_aggr", prop)
//    while(true) {
//      println("HELLO ")
//    }
  }
}


