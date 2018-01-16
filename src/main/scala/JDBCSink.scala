/**
  * Created by nvnw on 2018-01-15.
  */
package com.dhee

import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.Row


class JDBCSink(url: String, user:String, pwd:String) extends org.apache.spark.sql.ForeachWriter[org.apache.spark.sql.Row] {

  val driver = "org.postgresql.Driver"
  var connection:java.sql.Connection = _
  var statement:java.sql.Statement = _

  def open(partitionId: Long, version: Long):Boolean = {
    import java.sql.{Connection, DriverManager, ResultSet}
    classOf[org.postgresql.Driver]
    val conn = DriverManager.getConnection(url)
    statement = connection.createStatement
    true
  }

  def process(value: org.apache.spark.sql.Row): Unit = {

    val insertStmt = s""" Insert Into "TEST_AGGR" values ('${value(0)}', ${value(1)}) \n"""
    statement.executeUpdate(insertStmt)
  }

  def close(errorOrNull:Throwable):Unit = {
    connection.close
  }
}