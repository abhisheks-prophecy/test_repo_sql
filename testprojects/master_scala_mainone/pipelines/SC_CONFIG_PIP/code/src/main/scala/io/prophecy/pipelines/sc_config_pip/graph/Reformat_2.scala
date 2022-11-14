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

object Reformat_2 {

  def apply(spark: SparkSession, in: DataFrame): DataFrame =
    in.select(concat(lit(Config.c_string), col("`c  - int`")).as("c_config"))

}
