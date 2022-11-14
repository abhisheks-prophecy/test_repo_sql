package org.scala.limiting_nested

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Subgraph_Inner {

  def apply(spark: SparkSession, in0: DataFrame): DataFrame = {
    val df_Limit_Inside_Inner = Limit_Inside_Inner(spark, in0)
    df_Limit_Inside_Inner
  }

}
