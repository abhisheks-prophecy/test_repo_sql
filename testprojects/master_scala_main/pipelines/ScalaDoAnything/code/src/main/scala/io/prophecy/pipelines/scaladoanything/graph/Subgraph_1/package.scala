package io.prophecy.pipelines.scaladoanything.graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Subgraph_1 {

  def apply(spark: SparkSession, in0: DataFrame): DataFrame = {
    val df_Reformat_2 = Reformat_2(spark, in0)
    val df_Filter_2   = Filter_2(spark,   df_Reformat_2)
    df_Filter_2
  }

}
