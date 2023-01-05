package io.prophecy.pipelines.scala2_11_pipeline.graph

import io.prophecy.libs._
import io.prophecy.pipelines.scala2_11_pipeline.config.ConfigStore._
import io.prophecy.pipelines.scala2_11_pipeline.config.Context
import io.prophecy.pipelines.scala2_11_pipeline.udfs.UDFs._
import io.prophecy.pipelines.scala2_11_pipeline.udfs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Script_1 {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    print("Successfully Executed.")
    var out0=in0
    out0
  }

}
