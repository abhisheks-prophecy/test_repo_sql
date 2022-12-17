package graph.all_types

import io.prophecy.libs._
import config.Context
import graph.all_types.recursive.Subgraph_2
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object recursive {

  def apply(context: Context, in0: DataFrame): DataFrame = {
    val df_Reformat_3 = Reformat_3(context,       in0)
    val df_Subgraph_2 = Subgraph_2.apply(context, df_Reformat_3)
    df_Subgraph_2
  }

}
