package io.prophecy.pipelines.livy_scala.graph

import io.prophecy.libs._
import io.prophecy.pipelines.livy_scala.config.ConfigStore._
import io.prophecy.pipelines.livy_scala.config.Context
import io.prophecy.pipelines.livy_scala.graph.Subgraph_4.Subgraph_2_1
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Subgraph_4 {

  def apply(context: Context, in0: DataFrame): DataFrame = {
    val df_Reformat_2_1 = Reformat_2_1(context,       in0)
    val df_Filter_1_1   = Filter_1_1(context,         df_Reformat_2_1)
    val df_OrderBy_1_1  = OrderBy_1_1(context,        df_Filter_1_1)
    val df_Subgraph_2_1 = Subgraph_2_1.apply(context, df_OrderBy_1_1)
    df_Subgraph_2_1
  }

}
