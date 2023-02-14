package io.prophecy.pipelines.scala_livy

import io.prophecy.libs._
import io.prophecy.pipelines.scala_livy.config.ConfigStore._
import io.prophecy.pipelines.scala_livy.config.Context
import io.prophecy.pipelines.scala_livy.config._
import io.prophecy.pipelines.scala_livy.udfs.UDFs._
import io.prophecy.pipelines.scala_livy.udfs._
import io.prophecy.pipelines.scala_livy.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_src_livy_csv = src_livy_csv(context)
    val df_Reformat_1   = Reformat_1(context, df_src_livy_csv)
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SCALA_LIVY")
    MetricsCollector.start(spark,                    "pipelines/SCALA_LIVY")
    apply(context)
    MetricsCollector.end(spark)
  }

}
