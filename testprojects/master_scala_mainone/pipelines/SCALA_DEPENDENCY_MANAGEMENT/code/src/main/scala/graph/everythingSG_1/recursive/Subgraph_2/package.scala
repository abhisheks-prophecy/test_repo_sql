package graph.everythingSG_1.recursive

import io.prophecy.libs._
import graph.everythingSG_1.recursive.Subgraph_2.Subgraph_3
import graph.everythingSG_1.recursive.Subgraph_2.Subgraph_3.config.{
  Context => Subgraph_3_Context
}
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
import graph.everythingSG_1.recursive.Subgraph_2.config._
import graph.everythingSG_1.recursive.Subgraph_2.config.Config.interimOutput
package object Subgraph_2 {

  def apply(context: Context, in0: DataFrame): DataFrame = {
    val df_Subgraph_3 = Subgraph_3.apply(
      Subgraph_3_Context(context.spark, context.config.Subgraph_3),
      in0
    )
    val df_Reformat_4 = Reformat_4(context, df_Subgraph_3).interim(
      "Subgraph_2",
      "bpcR1sGqIR9SFuuY2OvAW$$OuKOVnN36ujpDehrhVjft",
      "qS_8hLjIGlQsdomnvr-KM$$DAfbiSoAUqr-KCG-SCKma"
    )
    df_Reformat_4
  }

}
