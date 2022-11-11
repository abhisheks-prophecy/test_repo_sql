package org.scala.limiting_nested.Subgraph_Inner

import io.prophecy.libs._
import config.ConfigStore._
import udfs.UDFs._
import udfs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Limit_Inside_Inner {
  def apply(spark: SparkSession, in: DataFrame): DataFrame = in.limit(50)
}
