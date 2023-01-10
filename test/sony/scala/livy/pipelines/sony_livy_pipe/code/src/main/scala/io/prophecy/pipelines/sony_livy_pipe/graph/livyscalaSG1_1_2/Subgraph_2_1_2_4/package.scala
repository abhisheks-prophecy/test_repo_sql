package io.prophecy.pipelines.sony_livy_pipe.graph.livyscalaSG1_1_2

import io.prophecy.libs._
import io.prophecy.pipelines.sony_livy_pipe.config.ConfigStore._
import io.prophecy.pipelines.sony_livy_pipe.config.Context
import io.prophecy.pipelines.sony_livy_pipe.graph.livyscalaSG1_1_2.Subgraph_2_1_2_4.Subgraph_3_1_2_4
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Subgraph_2_1_2_4 {

  def apply(context: Context, in0: DataFrame): DataFrame = {
    val df_Reformat_3_1_2_4 = Reformat_3_1_2_4(context, in0)
    val df_Subgraph_3_1_2_4 =
      Subgraph_3_1_2_4.apply(context, df_Reformat_3_1_2_4)
    df_Subgraph_3_1_2_4
  }

}
