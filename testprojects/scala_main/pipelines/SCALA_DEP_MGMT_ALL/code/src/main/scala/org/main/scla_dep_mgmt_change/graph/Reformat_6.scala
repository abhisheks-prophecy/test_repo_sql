package org.main.scla_dep_mgmt_change.graph

import io.prophecy.libs._
import org.main.scla_dep_mgmt_change.udfs.UDFs._
import org.main.scla_dep_mgmt_change.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Reformat_6 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(col("customer_id").as("c1"),
              col("customer_id"),
              col("customer_id").as("first_name")
    )

}
