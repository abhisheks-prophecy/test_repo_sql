package org.main.scla_dep_mgmt_change.graph.PERF_SG.recursive_1_1.Subgraph_2_1_1.Subgraph_3_1_1.Subgraph_4_1_1.Subgraph_1.Subgraph_2.Subgraph_3

import io.prophecy.libs._
import org.main.scla_dep_mgmt_change.config.ConfigStore._
import org.main.scla_dep_mgmt_change.config.Context
import org.main.scla_dep_mgmt_change.config._
import org.main.scla_dep_mgmt_change.graph.PERF_SG.recursive_1_1.Subgraph_2_1_1.Subgraph_3_1_1.Subgraph_4_1_1.Subgraph_1.Subgraph_2.Subgraph_3.Subgraph_4.Subgraph_5
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Subgraph_4 {

  def apply(context: Context, in0: DataFrame): DataFrame = {
    val df_Subgraph_5 = Subgraph_5.apply(context, in0)
    df_Subgraph_5
  }

}
