package io.prophecy.pipelines.testpipeline

import io.prophecy.libs._
import io.prophecy.pipelines.testpipeline.config.ConfigStore._
import io.prophecy.pipelines.testpipeline.config._
import io.prophecy.pipelines.testpipeline.udfs.UDFs._
import io.prophecy.pipelines.testpipeline.udfs._
import io.prophecy.pipelines.testpipeline.graph._
import io.prophecy.pipelines.testpipeline.graph.everythingSG_1
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
    val (df_everythingSG_1_out0,
         df_everythingSG_1_out1,
         df_everythingSG_1_out2,
         df_everythingSG_1_out3
    ) = everythingSG_1.apply(
      spark,
      df_src_parquet_all_type_and_partition_withspacehyphens,
      df_src_parquet_all_type_and_partition_withspacehyphens,
      df_src_parquet_all_type_and_partition_withspacehyphens
    )
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/TestPipeline1")
    MetricsCollector.start(
      spark,
      spark.conf.get("prophecy.project.id") + "/" + "pipelines/TestPipeline1"
    )
    apply(spark)
    MetricsCollector.end(spark)
  }

}
