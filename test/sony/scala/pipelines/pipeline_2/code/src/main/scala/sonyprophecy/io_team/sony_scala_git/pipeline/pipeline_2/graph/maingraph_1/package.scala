package sonyprophecy.io_team.sony_scala_git.pipeline.pipeline_2.graph

import io.prophecy.libs._
import sonyprophecy.io_team.sony_scala_git.pipeline.pipeline_2.graph.maingraph_1.graph2
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
import sonyprophecy.io_team.sony_scala_git.pipeline.pipeline_2.graph.maingraph_1.config._
package object maingraph_1 {

  def apply(context: Context, in0: DataFrame): DataFrame = {
    val df_Reformat_1_1_1 = Reformat_1_1_1(context, in0)
    val df_graph2 = graph2.apply(
      graph2.config.Context(context.spark, context.config.graph2),
      df_Reformat_1_1_1
    )
    df_graph2
  }

}
