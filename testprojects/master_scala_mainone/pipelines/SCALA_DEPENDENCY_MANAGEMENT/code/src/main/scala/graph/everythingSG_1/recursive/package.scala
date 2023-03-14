package graph.everythingSG_1

import io.prophecy.libs._
import graph.everythingSG_1.recursive.Subgraph_2
import graph.everythingSG_1.recursive.Subgraph_2.config.{
  Context => Subgraph_2_Context
}
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
import graph.everythingSG_1.recursive.config._
import graph.everythingSG_1.recursive.config.Config.interimOutput
package object recursive {

  def apply(context: Context, in0: DataFrame): DataFrame = {
    val df_Reformat_3 = Reformat_3(context, in0).interim(
      "recursive",
      "9y6m1ZdYcaG_UL4dNgN6H$$UfBi3yCfou0G4qTrMKqci",
      "gcqxL_GTQsTmJE5uAFZc2$$rHIjcOvrvOK26KWiIsSG1"
    )
    val df_Subgraph_2 = Subgraph_2.apply(
      Subgraph_2_Context(context.spark, context.config.Subgraph_2),
      df_Reformat_3
    )
    df_Subgraph_2
  }

}
