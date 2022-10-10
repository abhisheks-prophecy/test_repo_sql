package org.main.scla_dep_mgmt.graph

import io.prophecy.libs._
import org.main.scla_dep_mgmt.config.ConfigStore._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time._

object src_json_input_custs_1 {

  def apply(spark: SparkSession): DataFrame =
    spark.read
      .format("json")
      .schema(
        StructType(
          Array(
            StructField("account_flags",     StringType, true),
            StructField("account_open_date", StringType, true),
            StructField("country_code",      StringType, true),
            StructField("customer_id",       StringType, true),
            StructField("email",             StringType, true),
            StructField("first_name",        StringType, true),
            StructField("last_name",         StringType, true),
            StructField("phone",             StringType, true)
          )
        )
      )
      .load("dbfs:/Prophecy/qa_data/json/CustomersDatasetInput.json")
      .cache()

}
