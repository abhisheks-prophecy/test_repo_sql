package io.prophecy.pipelines.scala_azure.graph

import io.prophecy.libs._
import io.prophecy.pipelines.scala_azure.config.ConfigStore._
import io.prophecy.pipelines.scala_azure.udfs.UDFs._
import io.prophecy.pipelines.scala_azure.udfs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Reformat_1 {
  def apply(spark: SparkSession, in: DataFrame): DataFrame = in
}