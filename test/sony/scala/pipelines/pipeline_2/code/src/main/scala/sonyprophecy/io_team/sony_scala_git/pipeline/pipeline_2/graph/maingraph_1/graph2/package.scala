package sonyprophecy.io_team.sony_scala_git.pipeline.pipeline_2.graph.maingraph_1

import io.prophecy.libs._
import sonyprophecy.io_team.sony_scala_git.pipeline.pipeline_2.graph.maingraph_1.graph2.graph3
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
import sonyprophecy.io_team.sony_scala_git.pipeline.pipeline_2.graph.maingraph_1.graph2.config._
package object graph2 {

  def apply(context: Context, in0: DataFrame): DataFrame = {
    val df_Reformat_2 = Reformat_2(context, in0)
    val df_graph3 = graph3.apply(
      graph3.config.Context(context.spark, context.config.graph3),
      df_Reformat_2
    )
    df_graph3
  }

}
