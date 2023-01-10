package sonyprophecy.io_team.sony_scala_git.pipeline.pipeline_2

import io.prophecy.libs._
import sonyprophecy.io_team.sony_scala_git.pipeline.pipeline_2.config.Context
import sonyprophecy.io_team.sony_scala_git.pipeline.pipeline_2.config._
import sonyprophecy.io_team.sony_scala_git.pipeline.pipeline_2.udfs.UDFs._
import sonyprophecy.io_team.sony_scala_git.pipeline.pipeline_2.udfs._
import sonyprophecy.io_team.sony_scala_git.pipeline.pipeline_2.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {
  def apply(context: Context): Unit = {}

  def main(args:     Array[String]): Unit = {
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/pipeline_2")
    registerUDFs(spark)
    MetricsCollector.start(spark, "pipelines/pipeline_2")
    apply(context)
    MetricsCollector.end(spark)
  }

}
