package io.prophecy.pipelines.minimal

import io.prophecy.libs._
import io.prophecy.pipelines.minimal.config.ConfigStore._
import io.prophecy.pipelines.minimal.config.Context
import io.prophecy.pipelines.minimal.config._
import io.prophecy.pipelines.minimal.udfs.UDFs._
import io.prophecy.pipelines.minimal.udfs._
import io.prophecy.pipelines.minimal.graph._
import io.prophecy.pipelines.minimal.graph.Subgraph_4_1_1
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_livy_src_csv_1_1 = livy_src_csv_1_1(context)
    val df_Subgraph_4_1_1   = Subgraph_4_1_1.apply(context, df_livy_src_csv_1_1)
    val (df_RowDistributor_1_1_1_out0, df_RowDistributor_1_1_1_out1) = {
      val (df_RowDistributor_1_1_1_out0_temp,
           df_RowDistributor_1_1_1_out1_temp
      ) = RowDistributor_1_1_1(context, df_Subgraph_4_1_1)
      (df_RowDistributor_1_1_1_out0_temp.cache(),
       df_RowDistributor_1_1_1_out1_temp.cache()
      )
    }
    val df_Filter_2_1_1 = Filter_2_1_1(context, df_RowDistributor_1_1_1_out1)
    val df_Reformat_1   = Reformat_1(context,   df_Filter_2_1_1)
    val df_Reformat_5_1_1 =
      Reformat_5_1_1(context, df_RowDistributor_1_1_1_out0)
    val df_SchemaTransform_1_1_1 =
      SchemaTransform_1_1_1(context, df_Reformat_5_1_1)
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/minimal")
    MetricsCollector.start(spark,                    "pipelines/minimal")
    apply(context)
    MetricsCollector.end(spark)
  }

}
