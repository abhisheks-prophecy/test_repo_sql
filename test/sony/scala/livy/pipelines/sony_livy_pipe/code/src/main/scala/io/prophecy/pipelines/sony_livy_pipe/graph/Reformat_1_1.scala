package io.prophecy.pipelines.sony_livy_pipe.graph

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

object Reformat_1_1 {

  def apply(context: Context, in: DataFrame): DataFrame = {
    val Config = context.config
    in.select(
      col("year"),
      lookup("LookupTest", col("variable")).getField("value").as("lookup1"),
      col("industry_code_ANZSIC"),
      col("industry_name_ANZSIC"),
      col("rme_size_grp"),
      col("variable"),
      col("value"),
      col("unit"),
      concat(lit(Config.c_string),
             lit(Config.c_int),
             udf1(col("unit").cast(IntegerType))
      ).as("c_configs")
    )
  }

}
