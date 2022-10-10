package org.main.scla_dep_mgmt.graph.all_type_scala_sg_1.recursive_1.Subgraph_2_1

import io.prophecy.libs._
import org.main.scla_dep_mgmt.config.ConfigStore._
import org.main.scla_dep_mgmt.config._
import org.main.scla_dep_mgmt.graph.all_type_scala_sg_1.recursive_1.Subgraph_2_1.Subgraph_3_1.Subgraph_4_1
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time._
package object Subgraph_3_1 {

  def apply(spark: SparkSession, in0: DataFrame): DataFrame = {
    val df_Subgraph_4_1 = Subgraph_4_1.apply(spark, in0)
    val df_Reformat_5_1 = Reformat_5_1(spark, df_Subgraph_4_1).interim(
      "Subgraph_3_1",
      "Reformat_5_1",
      "Bohxyl3etMpUL76SFhG2z$$Xct9L4vY5rg7EOVZpmHKk"
    )
    df_Reformat_5_1
  }

}
