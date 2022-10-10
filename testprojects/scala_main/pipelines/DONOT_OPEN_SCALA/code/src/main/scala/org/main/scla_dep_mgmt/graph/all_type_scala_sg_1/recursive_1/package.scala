package org.main.scla_dep_mgmt.graph.all_type_scala_sg_1

import io.prophecy.libs._
import org.main.scla_dep_mgmt.config.ConfigStore._
import org.main.scla_dep_mgmt.config._
import org.main.scla_dep_mgmt.graph.all_type_scala_sg_1.recursive_1.Subgraph_2_1
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object recursive_1 {

  def apply(spark: SparkSession, in0: DataFrame): DataFrame = {
    val df_Reformat_3_1 = Reformat_3_1(spark, in0).interim(
      "recursive_1",
      "jiGNL3C_2hXv9zvRilmeP$$g96P8g-RqRLoYY44JOCMu",
      "k3m-wgXN7AdI1qeeM3boA$$Ggs7e_9S5UOmofHJaeLUS"
    )
    val df_Subgraph_2_1 = Subgraph_2_1.apply(spark, df_Reformat_3_1)
    df_Subgraph_2_1
  }

}
