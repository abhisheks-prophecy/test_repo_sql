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

object Reformat_3_1 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(col("id"),
              col("features"),
              col("hashes"),
              col("matrix_from_udf"),
              col("vector_from_udf"),
              udf_matrix(col("matrix_from_udf")).as("matrix_value_from_udf")
    )

}
