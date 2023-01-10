package org.main.scla_dep_mgmt.graph

import io.prophecy.libs._
import org.main.scla_dep_mgmt.config.ConfigStore._
import org.main.scla_dep_mgmt.config.Context
import org.main.scla_dep_mgmt.udfs.UDFs._
import org.main.scla_dep_mgmt.udfs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object OrderBy_2 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.orderBy(
      col("customer_id").asc,
      col("first_name").asc,
      col("last_name").asc,
      col("phone").asc,
      col("email").asc,
      col("country_code").asc,
      col("account_open_date").asc,
      col("account_flags").asc,
      col("config_values").desc
    )

}
