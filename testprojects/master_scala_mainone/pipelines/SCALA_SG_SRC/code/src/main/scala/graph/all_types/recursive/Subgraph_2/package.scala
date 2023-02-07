package graph.all_types.recursive

import io.prophecy.libs._
import config.ConfigStore._
import config.Context
import graph.all_types.recursive.Subgraph_2.Subgraph_3
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Subgraph_2 {

  def apply(context: Context, in0: DataFrame): DataFrame = {
    val df_Subgraph_3 = Subgraph_3.apply(context, in0)
    val df_Reformat_4 = Reformat_4(context,       df_Subgraph_3)
    df_Reformat_4
  }

}
