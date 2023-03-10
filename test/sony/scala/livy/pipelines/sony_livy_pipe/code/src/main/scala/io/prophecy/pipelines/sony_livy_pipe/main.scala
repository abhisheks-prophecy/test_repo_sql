package io.prophecy.pipelines.sony_livy_pipe

import io.prophecy.libs._
import io.prophecy.pipelines.sony_livy_pipe.config.ConfigStore._
import io.prophecy.pipelines.sony_livy_pipe.config.Context
import io.prophecy.pipelines.sony_livy_pipe.config._
import io.prophecy.pipelines.sony_livy_pipe.udfs.UDFs._
import io.prophecy.pipelines.sony_livy_pipe.udfs._
import io.prophecy.pipelines.sony_livy_pipe.graph._
import io.prophecy.pipelines.sony_livy_pipe.graph.livyscalaSG1_1_1
import io.prophecy.pipelines.sony_livy_pipe.graph.Subgraph_4_1_1
import io.prophecy.pipelines.sony_livy_pipe.graph.Subgraph_4_1
import io.prophecy.pipelines.sony_livy_pipe.graph.livyscalaSG1_1_1_1
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_livy_src_csv_1_1 = livy_src_csv_1_1(context)
    Lookup_1_1_1(context, df_livy_src_csv_1_1)
    val df_livy_src_csv_1 = livy_src_csv_1(context)
    Lookup_1_1(context, df_livy_src_csv_1)
    val df_livy_src_csv = livy_src_csv(context)
    val df_Reformat_2   = Reformat_2(context, df_livy_src_csv)
    Lookup_1(context, df_Reformat_2)
    val df_livy_src_csv_2_1_1 = livy_src_csv_2_1_1(context)
    val df_Reformat_1_1_1     = Reformat_1_1_1(context, df_livy_src_csv_1_1)
    val df_SetOperation_1_1_1 =
      SetOperation_1_1_1(context, df_Reformat_1_1_1, df_Reformat_1_1_1)
    val df_livy_src_csv_2_1 = livy_src_csv_2_1(context)
    val df_Reformat_1_1     = Reformat_1_1(context, df_livy_src_csv_1)
    val df_SetOperation_1_1 =
      SetOperation_1_1(context, df_Reformat_1_1, df_Reformat_1_1)
    val df_livyscalaSG1_1_1 =
      livyscalaSG1_1_1.apply(context, df_SetOperation_1_1)
    val df_livy_src_csv_2   = livy_src_csv_2(context)
    val df_Reformat_1_3     = Reformat_1_3(context,     df_livy_src_csv_2)
    val df_Reformat_3_1     = Reformat_3_1(context,     df_Reformat_1_3)
    val df_Reformat_4_1_1   = Reformat_4_1_1(context,   df_Reformat_3_1)
    val df_Reformat_4_1     = Reformat_4_1(context,     df_Reformat_4_1_1)
    val df_Reformat_1_3_1   = Reformat_1_3_1(context,   df_livy_src_csv_2_1)
    val df_Reformat_3_1_1   = Reformat_3_1_1(context,   df_Reformat_1_3_1)
    val df_Reformat_4_1_1_1 = Reformat_4_1_1_1(context, df_Reformat_3_1_1)
    val df_Reformat_4_1_4   = Reformat_4_1_4(context,   df_Reformat_4_1_1_1)
    val df_SetOperation_1 =
      SetOperation_1(context, df_Reformat_4_1, df_Reformat_4_1_4)
    val df_Filter_1       = Filter_1(context,             df_SetOperation_1)
    val df_Subgraph_4_1_1 = Subgraph_4_1_1.apply(context, df_livy_src_csv_1_1)
    val (df_RowDistributor_1_1_1_out0, df_RowDistributor_1_1_1_out1) = {
      val (df_RowDistributor_1_1_1_out0_temp,
           df_RowDistributor_1_1_1_out1_temp
      ) = RowDistributor_1_1_1(context, df_Subgraph_4_1_1)
      (df_RowDistributor_1_1_1_out0_temp.cache(),
       df_RowDistributor_1_1_1_out1_temp.cache()
      )
    }
    val df_Reformat_5_1_1 =
      Reformat_5_1_1(context, df_RowDistributor_1_1_1_out0)
    val df_livy_src_csv_2_3 = livy_src_csv_2_3(context)
    val df_Reformat_1_3_3   = Reformat_1_3_3(context,     df_livy_src_csv_2_3)
    val df_Subgraph_4_1     = Subgraph_4_1.apply(context, df_livy_src_csv_1)
    val (df_RowDistributor_1_1_out0, df_RowDistributor_1_1_out1) = {
      val (df_RowDistributor_1_1_out0_temp, df_RowDistributor_1_1_out1_temp) =
        RowDistributor_1_1(context, df_Subgraph_4_1)
      (df_RowDistributor_1_1_out0_temp.cache(),
       df_RowDistributor_1_1_out1_temp.cache()
      )
    }
    val df_Filter_2_1          = Filter_2_1(context,          df_RowDistributor_1_1_out1)
    val df_livy_src_csv_2_2    = livy_src_csv_2_2(context)
    val df_Reformat_1_3_2      = Reformat_1_3_2(context,      df_livy_src_csv_2_2)
    val df_Reformat_3_1_3      = Reformat_3_1_3(context,      df_Reformat_1_3_2)
    val df_Reformat_4_1_1_2    = Reformat_4_1_1_2(context,    df_Reformat_3_1_3)
    val df_SQLStatement_1      = SQLStatement_1(context,      df_livy_src_csv)
    val df_Reformat_3          = Reformat_3(context,          df_SQLStatement_1)
    val df_Reformat_5_1        = Reformat_5_1(context,        df_RowDistributor_1_1_out0)
    val df_SchemaTransform_1_1 = SchemaTransform_1_1(context, df_Reformat_5_1)
    val df_Filter_2_1_1        = Filter_2_1_1(context,        df_RowDistributor_1_1_1_out1)
    val df_Reformat_1_3_1_1    = Reformat_1_3_1_1(context,    df_livy_src_csv_2_1_1)
    val df_Reformat_4_1_5      = Reformat_4_1_5(context,      df_Reformat_4_1_1_2)
    val df_Reformat_3_1_1_1    = Reformat_3_1_1_1(context,    df_Reformat_1_3_1_1)
    val df_SchemaTransform_1_1_1 =
      SchemaTransform_1_1_1(context, df_Reformat_5_1_1)
    val df_WindowFunction_1 = WindowFunction_1(context, df_Reformat_3)
    val df_Filter_2         = Filter_2(context,         df_WindowFunction_1)
    val df_Script_1_1       = Script_1_1(context,       df_livyscalaSG1_1_1)
    val df_Reformat_6_1     = Reformat_6_1(context,     df_Script_1_1)
    val df_livyscalaSG1_1_1_1 =
      livyscalaSG1_1_1_1.apply(context, df_SetOperation_1_1_1)
    val df_Script_1_1_1       = Script_1_1_1(context,       df_livyscalaSG1_1_1_1)
    val df_Reformat_6_1_1     = Reformat_6_1_1(context,     df_Script_1_1_1)
    val df_Reformat_4_1_1_1_1 = Reformat_4_1_1_1_1(context, df_Reformat_3_1_1_1)
    val df_Reformat_4_1_4_1   = Reformat_4_1_4_1(context,   df_Reformat_4_1_1_1_1)
    val df_Reformat_4_1_3     = Reformat_4_1_3(context,     df_Filter_1)
    target_dataset(context, df_Reformat_4_1_3)
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
