/**
  * Created by Dheefinity on 2018-01-15.
  */
package com.dhee

import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.Row


class JDBCSink(url: String, user:String, pwd:String, tableName:String) extends org.apache.spark.sql.ForeachWriter[org.apache.spark.sql.Row] {

  val driver = "org.postgresql.Driver"
  var statement:java.sql.Statement = _
  var connection:java.sql.Connection = _

  def open(partitionId: Long, version: Long):Boolean = {
    //statement = connection.createStatement
    import java.sql.{Connection, DriverManager, ResultSet}
    classOf[org.postgresql.Driver]
    connection = DriverManager.getConnection(url)
    statement = connection.createStatement
    true
  }

  def process(value: org.apache.spark.sql.Row): Unit = {

    val insertStmt = s""" Insert Into ${tableName} (portfolio_id, scenario_id, sum_pnl) values ('${value(0)}', ${value(1)}, ${value(2)}) ON CONFLICT (portfolio_id, scenario_id) DO UPDATE SET sum_pnl = EXCLUDED.sum_pnl \n"""
    statement.executeUpdate(insertStmt)
  }

  def close(errorOrNull:Throwable):Unit = {
    connection.close
    //conn.close
  }
}
