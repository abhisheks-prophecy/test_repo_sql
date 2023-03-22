package com.scala.main.job1.graph

import io.prophecy.libs._
import com.scala.main.job1.udfs.UDFs._
import com.scala.main.job1.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Reformat_14 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("`nested.field.col1`").as("col1"),
      col("`nested.field.col2`.a.test1.`test- another`").as("test- another"),
      col("`nested.field.col2`.b.test1").as("test1")
    )

}
