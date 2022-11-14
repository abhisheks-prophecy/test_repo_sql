package io.prophecy.pipelines.sc_config_pip

import io.prophecy.libs._
import io.prophecy.pipelines.sc_config_pip.config.ConfigStore._
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

  def apply(spark: SparkSession): Unit = {
    val df_src_parquet_all_type_and_partition_withspacehyphens =
      src_parquet_all_type_and_partition_withspacehyphens(spark)
    val df_Reformat_2 =
      Reformat_2(spark, df_src_parquet_all_type_and_partition_withspacehyphens)
    val df_Reformat_1 =
      Reformat_1(spark, df_src_parquet_all_type_and_partition_withspacehyphens)
  }

  def main(args: Array[String]): Unit = {
    ConfigStore.Config = ConfigurationFactoryImpl.fromCLI(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Prophecy Pipeline")
      .config("spark.default.parallelism",             "4")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .enableHiveSupport()
      .getOrCreate()
      .newSession()
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SC_CONFIG_PIP")
    MetricsCollector.start(
      spark,
      spark.conf.get("prophecy.project.id") + "/" + "pipelines/SC_CONFIG_PIP"
    )
    apply(spark)
    MetricsCollector.end(spark)
  }

}
