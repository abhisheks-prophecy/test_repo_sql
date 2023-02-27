package io.prophecy.pipelines.sony_livy_pipe.graph.livyscalaSG1_1_1_1.Subgraph_2_1_2_2_1

import io.prophecy.libs._
import io.prophecy.pipelines.sony_livy_pipe.config.ConfigStore._
import io.prophecy.pipelines.sony_livy_pipe.config.Context
import io.prophecy.pipelines.sony_livy_pipe.udfs.UDFs._
import io.prophecy.pipelines.sony_livy_pipe.udfs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Reformat_3_1_2_2_1 {

  def apply(context: Context, in: DataFrame): DataFrame = {
    val Config = context.config
    in.select(
      col("year"),
      col("industry_code_ANZSIC"),
      col("industry_name_ANZSIC"),
      col("rme_size_grp"),
      col("variable"),
      col("value"),
      col("unit"),
      concat(lit(Config.c_string), lit(Config.c_int)).as("c_configs")
    )
  }

}