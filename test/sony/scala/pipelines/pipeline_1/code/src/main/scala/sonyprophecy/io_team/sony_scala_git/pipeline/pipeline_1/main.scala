package sonyprophecy.io_team.sony_scala_git.pipeline.pipeline_1

import io.prophecy.libs._
import sonyprophecy.io_team.sony_scala_git.pipeline.pipeline_1.config.Context
import sonyprophecy.io_team.sony_scala_git.pipeline.pipeline_1.config._
import sonyprophecy.io_team.sony_scala_git.pipeline.pipeline_1.udfs.UDFs._
import sonyprophecy.io_team.sony_scala_git.pipeline.pipeline_1.udfs._
import sonyprophecy.io_team.sony_scala_git.pipeline.pipeline_1.graph._
import sonyprophecy.io_team._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_git_dataet = git_dataet(context)
    val df_Reformat_1 = Reformat_1(context, df_git_dataet)
    val df_maingraph = sony_scala_git.subgraph.maingraph.apply(
      sony_scala_git.subgraph.maingraph.config
        .Context(context.spark, context.config.maingraph),
      df_Reformat_1
    )
  }

  def main(args: Array[String]): Unit = {
    val config = ConfigurationFactoryImpl.fromCLI(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Prophecy Pipeline")
      .config("spark.default.parallelism",             "4")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .enableHiveSupport()
      .getOrCreate()
      .newSession()
    val context = Context(spark, config)
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/pipeline_1")
    registerUDFs(spark)
    MetricsCollector.start(spark, "pipelines/pipeline_1")
    apply(context)
    MetricsCollector.end(spark)
  }

}
