package io.prophecy.pipelines.pipeline_scala211_spark24.graph

import io.prophecy.libs._
import io.prophecy.pipelines.pipeline_scala211_spark24.config.ConfigStore._
import io.prophecy.pipelines.pipeline_scala211_spark24.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object s3_source {

  def apply(context: Context): DataFrame =
    context.spark.read
      .format("parquet")
      .load("s3a://qa-prophecy/datasets/parquet/customers")

}
