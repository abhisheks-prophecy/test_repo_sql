package io.prophecy.pipelines.sony_livy_pipe

import io.prophecy.libs._
import io.prophecy.pipelines.sony_livy_pipe.config.ConfigStore._
import io.prophecy.pipelines.sony_livy_pipe.config.Context
import io.prophecy.pipelines.sony_livy_pipe.config._
import io.prophecy.pipelines.sony_livy_pipe.udfs.UDFs._
import io.prophecy.pipelines.sony_livy_pipe.udfs._
import io.prophecy.pipelines.sony_livy_pipe.graph._
import io.prophecy.pipelines.sony_livy_pipe.graph.livyscalaSG1_1
import io.prophecy.pipelines.sony_livy_pipe.graph.Subgraph_4
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_livy_src_csv = livy_src_csv(context)
    Lookup_1(context, df_livy_src_csv)
    val df_Reformat_1 = Reformat_1(context, df_livy_src_csv)
    val df_SetOperation_1 =
      SetOperation_1(context, df_Reformat_1, df_Reformat_1)
    val df_livyscalaSG1_1 = livyscalaSG1_1.apply(context, df_SetOperation_1)
    val df_Script_1       = Script_1(context,             df_livyscalaSG1_1)
    val df_Subgraph_4     = Subgraph_4.apply(context,     df_livy_src_csv)
    val (df_RowDistributor_1_out0, df_RowDistributor_1_out1) = {
      val (df_RowDistributor_1_out0_temp, df_RowDistributor_1_out1_temp) =
        RowDistributor_1(context, df_Subgraph_4)
      (df_RowDistributor_1_out0_temp.cache(),
       df_RowDistributor_1_out1_temp.cache()
      )
    }
    val df_Reformat_5        = Reformat_5(context,        df_RowDistributor_1_out0)
    val df_Filter_2          = Filter_2(context,          df_RowDistributor_1_out1)
    val df_SchemaTransform_1 = SchemaTransform_1(context, df_Reformat_5)
    val df_Reformat_6        = Reformat_6(context,        df_Script_1)
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/sony_livy_pipe")
    MetricsCollector.start(spark,                    "pipelines/sony_livy_pipe")
    apply(context)
    MetricsCollector.end(spark)
  }

}
