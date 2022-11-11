package graph.everythingSG_1.recursive

import io.prophecy.libs._
import config.ConfigStore._
import config._
import graph.everythingSG_1.recursive.Subgraph_2.Subgraph_3
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time._
package object Subgraph_2 {

  def apply(spark: SparkSession, in0: DataFrame): DataFrame = {
    val df_Subgraph_3 = Subgraph_3.apply(spark, in0)
    val df_Reformat_4 = Reformat_4(spark, df_Subgraph_3).interim(
      "Subgraph_2",
      "Reformat_4",
      "qS_8hLjIGlQsdomnvr-KM$$DAfbiSoAUqr-KCG-SCKma"
    )
    df_Reformat_4
  }

}
