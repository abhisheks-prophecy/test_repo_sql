package io.prophecy.pipelines.sony_livy_pipe.graph

import io.prophecy.libs._
import io.prophecy.pipelines.sony_livy_pipe.config.ConfigStore._
import io.prophecy.pipelines.sony_livy_pipe.config.Context
import io.prophecy.pipelines.sony_livy_pipe.graph.Subgraph_4.Subgraph_2_1_2
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Subgraph_4 {

  def apply(context: Context, in0: DataFrame): DataFrame = {
    val df_Reformat_2_1_2 = Reformat_2_1_2(context,       in0)
    val df_Filter_1_1_2   = Filter_1_1_2(context,         df_Reformat_2_1_2)
    val df_OrderBy_1_1_2  = OrderBy_1_1_2(context,        df_Filter_1_1_2)
    val df_Subgraph_2_1_2 = Subgraph_2_1_2.apply(context, df_OrderBy_1_1_2)
    df_Subgraph_2_1_2
  }

}
