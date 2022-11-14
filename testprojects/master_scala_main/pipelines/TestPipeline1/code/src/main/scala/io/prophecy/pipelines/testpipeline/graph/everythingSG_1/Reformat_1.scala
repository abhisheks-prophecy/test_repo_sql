package io.prophecy.pipelines.testpipeline.graph.everythingSG_1

import io.prophecy.libs._
import io.prophecy.pipelines.testpipeline.config.ConfigStore._
import io.prophecy.pipelines.testpipeline.udfs.UDFs._
import io.prophecy.pipelines.testpipeline.udfs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Reformat_1 {

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
      lookup("Lookup_1", col("customer_id"), col("first_name"))
        .getField("phone")
        .as("lookup1")
    )

}
