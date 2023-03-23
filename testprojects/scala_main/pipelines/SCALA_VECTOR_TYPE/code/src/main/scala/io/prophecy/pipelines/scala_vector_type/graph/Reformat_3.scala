package io.prophecy.pipelines.scala_vector_type.graph

import io.prophecy.libs._
import io.prophecy.pipelines.scala_vector_type.udfs.UDFs._
import io.prophecy.pipelines.scala_vector_type.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Reformat_3 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(col("id"),
              col("features"),
              col("hashes"),
              col("matrix_from_udf"),
              col("vector_from_udf"),
              udf_matrix(col("matrix_from_udf")).as("matrix_value_from_udf")
    )

}
