package com.scala.main.job1

import io.prophecy.libs._
import com.scala.main.job1.config.ConfigStore._
import com.scala.main.job1.config._
import com.scala.main.job1.udfs.UDFs._
import com.scala.main.job1.udfs._
import com.scala.main.job1.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def graph(spark: SparkSession): Unit = {
    val df_src_parquet_all_type_and_partition_withspacehyphens =
      src_parquet_all_type_and_partition_withspacehyphens(spark).interim(
        "graph",
        "o8K-lNobc6Z8Asi3dRegs$$Buw8lxPhFtSUcFZhGxXbx",
        "Yui765QKx0wOKHaOnjtzk$$Q04y-nxgyv0ZfORhocEUn"
      )
    val df_Reformat_1 = Reformat_1(
      spark,
      df_src_parquet_all_type_and_partition_withspacehyphens
    ).interim("graph",
              "bl47XMiEa-WNOlMEK4sFp$$2Dwp_ulOdXsBz8xjpntdm",
              "KHx3a7pEn7ZIJRnqGorWt$$RNdicUvC7gj0La9ZxSfE2"
    )
    df_Reformat_1.cache().count()
    df_Reformat_1.unpersist()
    val df_src_parquet_all_type_and_partition_withspacehyphens_1 =
      src_parquet_all_type_and_partition_withspacehyphens_1(spark).interim(
        "graph",
        "nWx5-SquUN7rAkxaPGDrk$$_nhpUgozXHNPGhSNNlqcK",
        "zHB_AHbuXxGulfLpf-XFk$$TIjxriDE51_-2XP8okB1E"
      )
    df_src_parquet_all_type_and_partition_withspacehyphens_1.cache().count()
    df_src_parquet_all_type_and_partition_withspacehyphens_1.unpersist()
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
    MetricsCollector.initializeMetrics(spark)
    implicit val interimOutputConsole: InterimOutput = InterimOutputHive2("")
    spark.conf.set("prophecy.collect.basic.stats",          "true")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    spark.conf.set("spark.sql.optimizer.excludedRules",
                   "org.apache.spark.sql.catalyst.optimizer.ColumnPruning"
    )
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SCALA_BASIC")
    MetricsCollector.start(spark,                    "pipelines/SCALA_BASIC")
    graph(spark)
    MetricsCollector.end(spark)
  }

}
