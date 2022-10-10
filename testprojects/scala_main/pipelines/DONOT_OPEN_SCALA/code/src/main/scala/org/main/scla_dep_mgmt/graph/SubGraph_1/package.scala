package org.main.scla_dep_mgmt.graph

import io.prophecy.libs._
import org.main.scla_dep_mgmt.config.ConfigStore._
import org.main.scla_dep_mgmt.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time._
package object SubGraph_1 {

  def apply(spark: SparkSession, in0: DataFrame): DataFrame = {
    val df_Reformat_9 = Reformat_9(spark, in0).interim(
      "SubGraph_1",
      "Reformat_9",
      "7PlOCNRfqR9BI6pdIHB8P$$uK8-618K17XoL4orppRDG"
    )
    df_Reformat_9
  }

}
