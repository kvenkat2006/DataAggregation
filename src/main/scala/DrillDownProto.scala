package com.dhee

//import org.apache.spark.sql.expressions.Aggregator
//import org.apache.spark.sql.Encoder
//import org.apache.spark.sql.Encoders
import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions._

//RanSud: Typesafe changes next 1 line
import com.typesafe.config.ConfigFactory

/// Add Exception handling in the code. There is hardly any, currently.

object DrillDownProto {
  def main(args: Array[String]): Unit = {

//RanSud: Typesafe changes next 4 lines
    val config = ConfigFactory.parseFile(new java.io.File("src/main/scala/dataagg.conf"))
    val db = config.getString("dataagg.config.consumer.dbname")

    val url = s"""jdbc:postgresql://localhost:5432/$db"""

    val prop = new java.util.Properties
    prop.setProperty("driver", "org.postgresql.Driver")

//RanSud: Typesafe changes next 5 lines
    val user = config.getString("dataagg.config.general.pgresuser")
    prop.setProperty("user", user)

    val pwd = config.getString("dataagg.config.general.pgrespw")
    prop.setProperty("password", pwd)

    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("Spark SQL DRILLDOWN")
      //.config("spark.some.config.option", "xxx")
      .getOrCreate()

//RanSud: Typesafe changes next 2 lines
    val baseDFpath = config.getString("dataagg.config.consumer.baseDFloc")
    val rawDataDF = spark.read.load(s"""$baseDFpath/*.parquet""")

    rawDataDF.createOrReplaceTempView("pnlStructTable")
    rawDataDF.printSchema()

//RanSud: Typesafe changes next 1 line
    val db_con_str = s"""jdbc:postgresql://localhost:5432/$db?user=$user&password=$pwd"""
    import java.sql.{Connection, DriverManager, ResultSet}
    classOf[org.postgresql.Driver]
    val pgconn = DriverManager.getConnection(db_con_str)
    val statement = pgconn.createStatement()
    val resultSet = statement.executeQuery(
      """
        | SELECT drilldown_request_id, filter_condition, drilldown_dim, processed
        | from drilldown_request_table
        | WHERE processed = FALSE
      """.stripMargin)

    while (resultSet.next()) {
      val drilldown_request_id = resultSet.getString("drilldown_request_id")
      val filter_condition = resultSet.getString("filter_condition")
      val drilldown_dim = resultSet.getString("drilldown_dim")
      val processed = resultSet.getString("processed")
      println(
        s"""drilldown_request_id: ${drilldown_request_id},
           |filter_condition: ${filter_condition},
           |drilldown_dim: ${drilldown_dim},
           |processed: ${processed},
       """.stripMargin)

      val drillDownDf = spark.sql(
        s"""select ${drilldown_request_id} as drilldown_request_id, ${drilldown_dim} as dim_1, scenarioId as scenario_id, sum(pnl) as dim_1_pnl
           |from pnlStructTable WHERE baseOrWhatif = 'BASE' and ${filter_condition}
           |group by drilldown_request_id, dim_1, scenario_id """.stripMargin)


      println(s"""select ${drilldown_request_id} as drilldown_request_id, ${drilldown_dim} as dim_1, scenarioId as scenario_id, sum(pnl) as dim_1_pnl
                 |from pnlStructTable WHERE baseOrWhatif = 'BASE' and ${filter_condition}
                 |group by drilldown_request_id, dim_1, scenario_id """.stripMargin)

      drillDownDf.printSchema()

      drillDownDf.write.mode("append").jdbc(url, "drilldown_table", prop)

      // Update drill_down_request_table, set processed = true for this request.
      val stmt = pgconn.createStatement()
      val updateStmt =
        s""" UPDATE drilldown_request_table set processed = TRUE
           | WHERE drilldown_request_id = ${drilldown_request_id}
         """.stripMargin
      stmt.executeUpdate(updateStmt)

    }


    pgconn.close()
  }
}
