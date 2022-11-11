package io.prophecy.pipelines.testpipeline.graph.everythingSG_1.recursive.Subgraph_2

import io.prophecy.libs._
import io.prophecy.pipelines.testpipeline.graph.everythingSG_1.recursive.Subgraph_2.Subgraph_3.Subgraph_4
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Subgraph_3 {

  def apply(spark: SparkSession, in0: DataFrame): DataFrame = {
    val df_Subgraph_4 = Subgraph_4.apply(spark, in0)
    val df_Reformat_5 = Reformat_5(spark,       df_Subgraph_4)
    df_Reformat_5
  }

}
