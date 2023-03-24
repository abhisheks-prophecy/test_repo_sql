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

object Reformat_2_1 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(col("id"),
              col("features"),
              col("hashes"),
              udf_matrices().as("matrix_from_udf"),
              udf_vectors().as("vector_from_udf")
    )

}
