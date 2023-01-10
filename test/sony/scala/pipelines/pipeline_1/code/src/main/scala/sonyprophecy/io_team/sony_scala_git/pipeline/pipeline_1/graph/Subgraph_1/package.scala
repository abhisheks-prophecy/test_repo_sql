package sonyprophecy.io_team.sony_scala_git.pipeline.pipeline_1.graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
import sonyprophecy.io_team.sony_scala_git.pipeline.pipeline_1.graph.Subgraph_1.config._
package object Subgraph_1 {

  def apply(context: Context, in0: DataFrame): DataFrame = {
    val df_Reformat_1_1 = Reformat_1_1(context, in0)
    df_Reformat_1_1
  }

}
