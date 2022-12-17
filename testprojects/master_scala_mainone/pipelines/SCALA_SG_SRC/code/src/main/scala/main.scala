import io.prophecy.libs._
import config.Context
import config._
import udfs.UDFs._
import udfs._
import graph._
import graph.all_types
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_src_parquet_all_type_and_partition_withspacehyphens =
      src_parquet_all_type_and_partition_withspacehyphens(context)
    val (df_all_types_out0,
         df_all_types_out1,
         df_all_types_out2,
         df_all_types_out3
    ) = all_types.apply(
      context,
      df_src_parquet_all_type_and_partition_withspacehyphens,
      df_src_parquet_all_type_and_partition_withspacehyphens,
      df_src_parquet_all_type_and_partition_withspacehyphens
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SCALA_SG_SRC")
    MetricsCollector.start(
      spark,
      spark.conf.get("prophecy.project.id") + "/" + "pipelines/SCALA_SG_SRC"
    )
    apply(context)
    MetricsCollector.end(spark)
  }

}
