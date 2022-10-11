package org.scala.livy.pipelines.livy_scala.graph

import io.prophecy.libs._
import io.prophecy.pipelines.livy_scala.graph.livyscalaSG1_1.Subgraph_2_1
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object livyscalaSG1_1 {

  def apply(spark: SparkSession, in0: DataFrame): DataFrame = {
    val df_Reformat_2_1 = Reformat_2_1(spark,       in0)
    val df_Filter_1_1   = Filter_1_1(spark,         df_Reformat_2_1)
    val df_OrderBy_1_1  = OrderBy_1_1(spark,        df_Filter_1_1)
    val df_Subgraph_2_1 = Subgraph_2_1.apply(spark, df_OrderBy_1_1)
    df_Subgraph_2_1
  }

}
