package io.prophecy.pipelines.sc_config_pip

import io.prophecy.libs._
import io.prophecy.pipelines.sc_config_pip.config.ConfigStore._
import io.prophecy.pipelines.sc_config_pip.config.Context
import io.prophecy.pipelines.sc_config_pip.config._
import io.prophecy.pipelines.sc_config_pip.udfs.UDFs._
import io.prophecy.pipelines.sc_config_pip.udfs._
import io.prophecy.pipelines.sc_config_pip.graph._
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
    val df_Reformat_2 = Reformat_2(
      context,
      df_src_parquet_all_type_and_partition_withspacehyphens
    )
    val df_Reformat_1 = Reformat_1(
      context,
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SC_CONFIG_PIP")
    MetricsCollector.start(spark,                    "pipelines/SC_CONFIG_PIP")
    apply(context)
    MetricsCollector.end(spark)
  }

}
