package graph.everythingSG_1

import io.prophecy.libs._
import config.ConfigStore._
import config._
import graph.everythingSG_1.recursive.Subgraph_2
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object recursive {

  def apply(spark: SparkSession, in0: DataFrame): DataFrame = {
    val df_Reformat_3 = Reformat_3(spark, in0).interim(
      "recursive",
      "9y6m1ZdYcaG_UL4dNgN6H$$UfBi3yCfou0G4qTrMKqci",
      "gcqxL_GTQsTmJE5uAFZc2$$rHIjcOvrvOK26KWiIsSG1"
    )
    val df_Subgraph_2 = Subgraph_2.apply(spark, df_Reformat_3)
    df_Subgraph_2
  }

}