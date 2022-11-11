package io.prophecy.pipelines.testpipeline.graph.everythingSG_1

import io.prophecy.libs._
import io.prophecy.pipelines.testpipeline.graph.everythingSG_1.recursive.Subgraph_2
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object recursive {

  def apply(spark: SparkSession, in0: DataFrame): DataFrame = {
    val df_Reformat_3 = Reformat_3(spark,       in0)
    val df_Subgraph_2 = Subgraph_2.apply(spark, df_Reformat_3)
    df_Subgraph_2
  }

}
