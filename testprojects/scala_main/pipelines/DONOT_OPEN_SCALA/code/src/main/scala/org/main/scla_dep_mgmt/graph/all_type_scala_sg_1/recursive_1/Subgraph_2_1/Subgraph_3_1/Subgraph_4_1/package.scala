package org.main.scla_dep_mgmt.graph.all_type_scala_sg_1.recursive_1.Subgraph_2_1.Subgraph_3_1

import io.prophecy.libs._
import org.main.scla_dep_mgmt.config.ConfigStore._
import org.main.scla_dep_mgmt.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time._
package object Subgraph_4_1 {

  def apply(spark: SparkSession, in0: DataFrame): DataFrame = {
    val df_Reformat_6_1 = Reformat_6_1(spark, in0).interim(
      "Subgraph_4_1",
      "Reformat_6_1",
      "napEovIX7XPJo3LvhF_EI$$kBZukmFE86Pza1E-8eFe7"
    )
    df_Reformat_6_1
  }

}