package org.scala

import io.prophecy.libs._
import graph.Limiting_Nested_Subgraph_1.Subgraph_Inner
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object limiting_nested {

  def apply(spark: SparkSession, in0: DataFrame): DataFrame = {
    val df_Limit_Inside_Outer = Limit_Inside_Outer(spark,   in0)
    val df_Subgraph_Inner     = Subgraph_Inner.apply(spark, df_Limit_Inside_Outer)
    df_Subgraph_Inner
  }

}
