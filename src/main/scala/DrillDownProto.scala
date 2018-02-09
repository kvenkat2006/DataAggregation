package com.dhee

//import org.apache.spark.sql.expressions.Aggregator
//import org.apache.spark.sql.Encoder
//import org.apache.spark.sql.Encoders
import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions._

/// Add Exception handling in the code. There is hardly any, currently.


object DrillDownProto {
  def main(args: Array[String]): Unit = {
    val url = "jdbc:postgresql://localhost:5432/DATA_AGGR"

    val prop = new java.util.Properties
    prop.setProperty("driver", "org.postgresql.Driver")
    prop.setProperty("user", "developer")
    prop.setProperty("password", "developer")

    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("Spark SQL DRILLDOWN")
      //.config("spark.some.config.option", "xxx")
      .getOrCreate()

    val rawDataDF = spark.read.load("/home/kumar/DataAggregation/baseDfData/*.parquet")
    rawDataDF.createOrReplaceTempView("pnlStructTable")
    rawDataDF.printSchema()


    val db_con_str = s"""jdbc:postgresql://localhost:5432/DATA_AGGR?user=developer&password=developer"""
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
