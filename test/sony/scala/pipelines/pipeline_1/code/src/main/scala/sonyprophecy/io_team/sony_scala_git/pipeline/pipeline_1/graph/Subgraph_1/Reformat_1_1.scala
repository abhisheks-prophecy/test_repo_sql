package sonyprophecy.io_team.sony_scala_git.pipeline.pipeline_1.graph.Subgraph_1

import io.prophecy.libs._
import sonyprophecy.io_team.sony_scala_git.functions._
import sonyprophecy.io_team.sony_scala_git.pipeline.pipeline_1.graph.Subgraph_1.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Reformat_1_1 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(col("_c0"), col("_c2"))

}