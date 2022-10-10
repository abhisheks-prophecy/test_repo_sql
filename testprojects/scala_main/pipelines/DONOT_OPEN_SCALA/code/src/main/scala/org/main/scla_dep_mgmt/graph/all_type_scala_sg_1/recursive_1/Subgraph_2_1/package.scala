package org.main.scla_dep_mgmt.graph.all_type_scala_sg_1.recursive_1

import io.prophecy.libs._
import org.main.scla_dep_mgmt.config.ConfigStore._
import org.main.scla_dep_mgmt.config._
import org.main.scla_dep_mgmt.graph.all_type_scala_sg_1.recursive_1.Subgraph_2_1.Subgraph_3_1
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time._
package object Subgraph_2_1 {

  def apply(spark: SparkSession, in0: DataFrame): DataFrame = {
    val df_Subgraph_3_1 = Subgraph_3_1.apply(spark, in0)
    val df_Reformat_4_1 = Reformat_4_1(spark, df_Subgraph_3_1).interim(
      "Subgraph_2_1",
      "Reformat_4_1",
      "fm5CFh16l0ffbWK_W8ULz$$wBdiA19LT64ghYdN8eP28"
    )
    df_Reformat_4_1
  }

}
