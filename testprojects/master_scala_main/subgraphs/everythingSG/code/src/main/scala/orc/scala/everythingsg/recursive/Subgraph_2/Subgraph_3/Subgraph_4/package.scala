package orc.scala.everythingsg.recursive.Subgraph_2.Subgraph_3

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Subgraph_4 {

  def apply(spark: SparkSession, in0: DataFrame): DataFrame = {
    val df_Reformat_6 = Reformat_6(spark, in0)
    df_Reformat_6
  }

}
