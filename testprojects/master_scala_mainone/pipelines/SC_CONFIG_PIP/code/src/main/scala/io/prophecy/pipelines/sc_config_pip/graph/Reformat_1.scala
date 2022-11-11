package io.prophecy.pipelines.sc_config_pip.graph

import io.prophecy.libs._
import io.prophecy.pipelines.sc_config_pip.config.ConfigStore._
import io.prophecy.pipelines.sc_config_pip.udfs.UDFs._
import io.prophecy.pipelines.sc_config_pip.udfs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Reformat_1 {

  def apply(spark: SparkSession, in: DataFrame): DataFrame =
    in.select(
      concat(lit(Config.c_string), col("`c  - int`")).as("c1_string"),
      concat(col("`c  - int`"),    lit(Config.c_boolean)).as("c2_boolean"),
      concat(col("`c  - int`"),    lit(Config.c_double)).as("c3_double"),
      concat(col("`c  - int`"),    lit(Config.c_float)).as("c4_float"),
      concat(col("`c  - int`"),    lit(Config.c_short)).as("c5_short"),
      concat(col("`c  - int`"),    lit(Config.c_int)).as("c6_int"),
      expr(Config.c_spark_expression).as("c6_spark_expr")
    )

}
