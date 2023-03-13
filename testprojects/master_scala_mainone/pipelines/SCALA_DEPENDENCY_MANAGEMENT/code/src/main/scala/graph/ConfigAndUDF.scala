package graph

import io.prophecy.libs._
import udfs.UDFs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object ConfigAndUDF {

  def apply(context: Context, in: DataFrame): DataFrame = {
    val Config = context.config
    in.select(
      col("customer_id"),
      col("first_name"),
      col("last_name"),
      col("phone"),
      col("email"),
      col("country_code"),
      col("account_open_date"),
      col("account_flags"),
      concat(
        col("customer_id"),
        lit(Config.c_array_string(0)),
        lit(Config.CONFIG_STR),
        lit(Config.CONFIG_BOOLEAN),
        lit(Config.CONFIG_DOUBLE),
        lit(Config.CONFIG_INT),
        lit(Config.CONFIG_FLOAT)
      ).as("config_values"),
      udf_multiply(col("customer_id").cast(IntegerType))
        .as("udf_multiply_usage"),
      udf_string_null_safe(col("first_name")).as("udf_string_null_safe_usage"),
      concat(lit(Config.c_record_complex.cr_array(0).crar_short),
             lit(Config.c_record_complex.cr_record.crr_array_bool(0))
      ).as("complex_configs"),
      concat(get_json_object(lit("{\"a\":\"b\"}"), "$.a"),
             lit(Config.c_string_with_dollar)
      ).as("expression_with_dollar")
    )
  }

}
