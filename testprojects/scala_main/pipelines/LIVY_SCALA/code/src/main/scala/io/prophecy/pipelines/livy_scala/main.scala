package io.prophecy.pipelines.livy_scala

import io.prophecy.libs._
import io.prophecy.pipelines.livy_scala.config.ConfigStore._
import io.prophecy.pipelines.livy_scala.config._
import io.prophecy.pipelines.livy_scala.udfs.UDFs._
import io.prophecy.pipelines.livy_scala.udfs._
import io.prophecy.pipelines.livy_scala.graph._
import io.prophecy.pipelines.livy_scala.graph.Subgraph_4
import io.prophecy.pipelines.livy_scala.graph.livyscalaSG1_1
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(spark: SparkSession): Unit = {
    val df_livy_src_csv = livy_src_csv(spark)
    Lookup_1(spark, df_livy_src_csv)
    val df_Reformat_1     = Reformat_1(spark,           df_livy_src_csv)
    val df_Source_1       = Source_1(spark)
    val df_Subgraph_4     = Subgraph_4.apply(spark,     df_livy_src_csv)
    val df_SetOperation_1 = SetOperation_1(spark,       df_Reformat_1, df_Reformat_1)
    val df_livyscalaSG1_1 = livyscalaSG1_1.apply(spark, df_SetOperation_1)
    val df_Script_1       = Script_1(spark,             df_livyscalaSG1_1)
    val df_Reformat_6     = Reformat_6(spark,           df_Script_1)
    val (df_RowDistributor_1_out0, df_RowDistributor_1_out1) =
      RowDistributor_1(spark, df_Subgraph_4)
    val df_Reformat_5        = Reformat_5(spark,        df_RowDistributor_1_out0)
    val df_SchemaTransform_1 = SchemaTransform_1(spark, df_Reformat_5)
    val df_Filter_2          = Filter_2(spark,          df_RowDistributor_1_out1)
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/LIVY_SCALA")
    MetricsCollector.start(
      spark,
      spark.conf.get("prophecy.project.id") + "/" + "pipelines/LIVY_SCALA"
    )
    apply(spark)
    MetricsCollector.end(spark)
  }

}
