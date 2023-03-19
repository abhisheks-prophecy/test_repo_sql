package com.main.sub_graph_src1.graph.all_type_sg_scala_main

import io.prophecy.libs._
import com.main.sub_graph_src1.graph.all_type_sg_scala_main.recursive_1.Subgraph_2_1
import com.main.sub_graph_src1.graph.all_type_sg_scala_main.recursive_1.Subgraph_2_1.config.{
  Context => Subgraph_2_1_Context
}
import com.main.sub_graph_src1.graph.all_type_sg_scala_main.recursive_1.config._
import com.main.sub_graph_src1.graph.all_type_sg_scala_main.recursive_1.config.Config.interimOutput
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object recursive_1 {

  def apply(context: Context, in0: DataFrame): DataFrame = {
    val df_Reformat_3_1 = Reformat_3_1(context, in0).interim(
      "recursive_1",
      "jiGNL3C_2hXv9zvRilmeP$$ESSC4eRR6Ot-f_rIcw_pP",
      "k3m-wgXN7AdI1qeeM3boA$$XARlawNbqKAnTSVK-m1Ub"
    )
    val df_Subgraph_2_1 = Subgraph_2_1.apply(
      Subgraph_2_1_Context(context.spark, context.config.Subgraph_2_1),
      df_Reformat_3_1
    )
    df_Subgraph_2_1
  }

}
