package org.main.scla_dep_mgmt_change.graph

import io.prophecy.libs._
import org.main.scla_dep_mgmt_change.config.ConfigStore._
import org.main.scla_dep_mgmt_change.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object PERF_SRC_2k_COLS {

  def apply(context: Context): DataFrame =
    context.spark.read
      .format("parquet")
      .load("dbfs:/Prophecy/qa_data/parquet/2000columns.parquet")

}
