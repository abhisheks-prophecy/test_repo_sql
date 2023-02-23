package io.prophecy.pipelines.scala_pipeline

import io.prophecy.libs._
import io.prophecy.pipelines.scala_pipeline.config.ConfigStore._
import io.prophecy.pipelines.scala_pipeline.config.Context
import io.prophecy.pipelines.scala_pipeline.config._
import io.prophecy.pipelines.scala_pipeline.udfs.UDFs._
import io.prophecy.pipelines.scala_pipeline.udfs._
import io.prophecy.pipelines.scala_pipeline.graph._
import io.prophecy.pipelines.scala_pipeline.graph.Subgraph_1
import io.prophecy.pipelines.scala_pipeline.graph.SCALA_SG
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_dataset_cust_in = dataset_cust_in(context)
    val df_Reformat_1      = Reformat_1(context,       df_dataset_cust_in)
    val df_Subgraph_1      = Subgraph_1.apply(context, df_Reformat_1)
    val df_SCALA_SG        = SCALA_SG.apply(context,   df_Reformat_1)
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SCALA_PIPELINE")
    MetricsCollector.start(spark,                    "pipelines/SCALA_PIPELINE")
    apply(context)
    MetricsCollector.end(spark)
  }

}
