package com.scala.main.job1.graph

import io.prophecy.libs._
import com.scala.main.job1.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object src_jdbc_userandpass_test_table {

  def apply(context: Context): DataFrame = {
    var reader = context.spark.read
      .format("jdbc")
      .option("url",      "jdbc:mysql://18.144.156.219:3306/test_database")
      .option("user",     "test_user")
      .option("password", "admin")
      .option("dbtable",  "test_table")
    reader = reader
      .option("pushDownPredicate", true)
      .option("driver",            "com.mysql.jdbc.Driver")
    reader.load()
  }

}
