package io.prophecy.pipelines.scala_vector_type.graph.Subgraph_1

import io.prophecy.libs._
import io.prophecy.pipelines.scala_vector_type.udfs.UDFs._
import io.prophecy.pipelines.scala_vector_type.graph.Subgraph_1.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Limit_2 {
  def apply(context: Context, in: DataFrame): DataFrame = in.limit(10)
}
