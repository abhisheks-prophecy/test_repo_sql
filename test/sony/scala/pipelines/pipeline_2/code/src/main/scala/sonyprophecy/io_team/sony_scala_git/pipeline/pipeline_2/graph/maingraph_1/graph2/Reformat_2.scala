package sonyprophecy.io_team.sony_scala_git.pipeline.pipeline_2.graph.maingraph_1.graph2

import io.prophecy.libs._
import sonyprophecy.io_team.sony_scala_git.functions._
import sonyprophecy.io_team.sony_scala_git.pipeline.pipeline_2.graph.maingraph_1.graph2.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Reformat_2 {

  def apply(context: Context, in: DataFrame): DataFrame = {
    val Config = context.config
    in.select(lit(Config.added_from_inside_graph3_var3).as("_c0"),
              lit(Config.graph2_newvar).as("_c2"),
              lit(Config.graph2_var2).as("_c3")
    )
  }

}
