package sonyprophecy.io_team.sony_scala_git.pipeline.pipeline_1.graph

import io.prophecy.libs._
import sonyprophecy.io_team.sony_scala_git.pipeline.pipeline_1.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object git_dataet {

  def apply(context: Context): DataFrame =
    context.spark.read
      .format("csv")
      .option("header", true)
      .option("sep",    ",")
      .load("dbfs:/Prophecy/qa_data/csv/all_type_no_partition")

}
