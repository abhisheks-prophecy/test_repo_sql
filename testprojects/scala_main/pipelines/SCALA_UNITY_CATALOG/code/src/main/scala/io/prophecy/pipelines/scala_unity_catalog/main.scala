package io.prophecy.pipelines.scala_unity_catalog

import io.prophecy.libs._
import io.prophecy.pipelines.scala_unity_catalog.config.ConfigStore._
import io.prophecy.pipelines.scala_unity_catalog.config.Context
import io.prophecy.pipelines.scala_unity_catalog.config._
import io.prophecy.pipelines.scala_unity_catalog.udfs.UDFs._
import io.prophecy.pipelines.scala_unity_catalog.udfs._
import io.prophecy.pipelines.scala_unity_catalog.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_src_parquet_unity_catalog = src_parquet_unity_catalog(context)
    val df_all_type_parquet_1        = all_type_parquet_1(context)
    val df_Filter_1                  = Filter_1(context,   df_all_type_parquet_1)
    val df_Reformat_1                = Reformat_1(context, df_src_parquet_unity_catalog)
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
    spark.conf
      .set("prophecy.metadata.pipeline.uri", "pipelines/SCALA_UNITY_CATALOG")
    MetricsCollector.start(spark,            "pipelines/SCALA_UNITY_CATALOG")
    apply(context)
    MetricsCollector.end(spark)
  }

}
