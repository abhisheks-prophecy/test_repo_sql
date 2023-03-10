package graph.everythingSG_1.recursive.Subgraph_2

import io.prophecy.libs._
import graph.everythingSG_1.recursive.Subgraph_2.Subgraph_3.Subgraph_4
import graph.everythingSG_1.recursive.Subgraph_2.Subgraph_3.Subgraph_4.config.{
  Context => Subgraph_4_Context
}
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
import graph.everythingSG_1.recursive.Subgraph_2.Subgraph_3.config._
import graph.everythingSG_1.recursive.Subgraph_2.Subgraph_3.config.Config.interimOutput
package object Subgraph_3 {

  def apply(context: Context, in0: DataFrame): DataFrame = {
    val df_Subgraph_4 = Subgraph_4.apply(
      Subgraph_4_Context(context.spark, context.config.Subgraph_4),
      in0
    )
    val df_Reformat_5 = Reformat_5(context, df_Subgraph_4).interim(
      "Subgraph_3",
      "M8Oi2KcDcOGc5kJ4vvDtX$$pnnJDnMbcDPlI4MUw5lIJ",
      "r86tzcI-HuWEQDCeDgsaB$$_FO3voLaDHv4ECFwVhqzO"
    )
    df_Reformat_5
  }

}
