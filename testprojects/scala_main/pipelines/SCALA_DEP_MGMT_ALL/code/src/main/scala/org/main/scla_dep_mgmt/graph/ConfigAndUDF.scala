package org.main.scla_dep_mgmt.graph

import io.prophecy.libs._
import org.main.scla_dep_mgmt.config.ConfigStore._
import org.main.scla_dep_mgmt.udfs.UDFs._
import org.main.scla_dep_mgmt.udfs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object ConfigAndUDF {

  def apply(spark: SparkSession, in: DataFrame): DataFrame =
    in.select(
      col("customer_id"),
      col("first_name"),
      col("last_name"),
      col("phone"),
      col("email"),
      col("country_code"),
      col("account_open_date"),
      col("account_flags"),
      concat(col("customer_id"),
             lit(Config.CONFIG_STR),
             lit(Config.CONFIG_BOOLEAN),
             lit(Config.CONFIG_DOUBLE),
             lit(Config.CONFIG_INT),
             lit(Config.CONFIG_FLOAT)
      ).as("config_values"),
      udf_multiply(col("customer_id").cast(IntegerType))
        .as("udf_multiply_usage"),
      udf_string_null_safe(col("first_name")).as("udf_string_null_safe_usage"),
      concat(lit(Config.c_str), col("first_name")).as("c_config")
    )

}
