package io.prophecy.pipelines.sony_livy_pipe

import io.prophecy.libs._
import io.prophecy.pipelines.sony_livy_pipe.config.ConfigStore._
import io.prophecy.pipelines.sony_livy_pipe.config.Context
import io.prophecy.pipelines.sony_livy_pipe.config._
import io.prophecy.pipelines.sony_livy_pipe.udfs.UDFs._
import io.prophecy.pipelines.sony_livy_pipe.udfs._
import io.prophecy.pipelines.sony_livy_pipe.graph._
import io.prophecy.pipelines.sony_livy_pipe.graph.livyscalaSG1_1_1
import io.prophecy.pipelines.sony_livy_pipe.graph.Subgraph_4_2
import io.prophecy.pipelines.sony_livy_pipe.graph.Subgraph_4_1_1
import io.prophecy.pipelines.sony_livy_pipe.graph.livyscalaSG1_1
import io.prophecy.pipelines.sony_livy_pipe.graph.Subgraph_4
import io.prophecy.pipelines.sony_livy_pipe.graph.livyscalaSG1_1_2
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
    Lookup_1(context, df_livy_src_csv)
    val df_livy_src_csv_2 = livy_src_csv_2(context)
    Lookup_1_2(context, df_livy_src_csv_2)
    val df_Reformat_1_1_1 = Reformat_1_1_1(context, df_livy_src_csv_1_1)
    val df_SetOperation_1_1_1 =
      SetOperation_1_1_1(context, df_Reformat_1_1_1, df_Reformat_1_1_1)
    val df_Reformat_1_2 = Reformat_1_2(context, df_livy_src_csv_2)
    val df_Reformat_1_1 = Reformat_1_1(context, df_livy_src_csv_1)
    val df_SetOperation_1_1 =
      SetOperation_1_1(context, df_Reformat_1_1, df_Reformat_1_1)
    val df_livyscalaSG1_1_1 =
      livyscalaSG1_1_1.apply(context, df_SetOperation_1_1)
    val df_Subgraph_4_2 = Subgraph_4_2.apply(context, df_livy_src_csv_2)
    val (df_RowDistributor_1_2_out0, df_RowDistributor_1_2_out1) = {
      val (df_RowDistributor_1_2_out0_temp, df_RowDistributor_1_2_out1_temp) =
        RowDistributor_1_2(context, df_Subgraph_4_2)
      (df_RowDistributor_1_2_out0_temp.cache(),
       df_RowDistributor_1_2_out1_temp.cache()
      )
    }
    val df_Filter_2_2     = Filter_2_2(context,           df_RowDistributor_1_2_out1)
    val df_Subgraph_4_1_1 = Subgraph_4_1_1.apply(context, df_livy_src_csv_1_1)
    val (df_RowDistributor_1_1_1_out0, df_RowDistributor_1_1_1_out1) = {
      val (df_RowDistributor_1_1_1_out0_temp,
           df_RowDistributor_1_1_1_out1_temp
      ) = RowDistributor_1_1_1(context, df_Subgraph_4_1_1)
      (df_RowDistributor_1_1_1_out0_temp.cache(),
       df_RowDistributor_1_1_1_out1_temp.cache()
      )
    }
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
    val df_Reformat_5 = Reformat_5(context, df_RowDistributor_1_out0)
    val df_Reformat_5_1_1 =
      Reformat_5_1_1(context, df_RowDistributor_1_1_1_out0)
    val df_SetOperation_1_2 =
      SetOperation_1_2(context, df_Reformat_1_2, df_Reformat_1_2)
    val df_livyscalaSG1_1_2 =
      livyscalaSG1_1_2.apply(context, df_SetOperation_1_2)
    val df_Script_1_2   = Script_1_2(context,         df_livyscalaSG1_1_2)
    val df_Subgraph_4_1 = Subgraph_4_1.apply(context, df_livy_src_csv_1)
    val (df_RowDistributor_1_1_out0, df_RowDistributor_1_1_out1) = {
      val (df_RowDistributor_1_1_out0_temp, df_RowDistributor_1_1_out1_temp) =
        RowDistributor_1_1(context, df_Subgraph_4_1)
      (df_RowDistributor_1_1_out0_temp.cache(),
       df_RowDistributor_1_1_out1_temp.cache()
      )
    }
    val df_Filter_2_1          = Filter_2_1(context,          df_RowDistributor_1_1_out1)
    val df_Reformat_5_1        = Reformat_5_1(context,        df_RowDistributor_1_1_out0)
    val df_SchemaTransform_1_1 = SchemaTransform_1_1(context, df_Reformat_5_1)
    val df_Filter_2_1_1        = Filter_2_1_1(context,        df_RowDistributor_1_1_1_out1)
    val df_Filter_2            = Filter_2(context,            df_RowDistributor_1_out1)
    val df_SchemaTransform_1   = SchemaTransform_1(context,   df_Reformat_5)
    val df_Reformat_5_2        = Reformat_5_2(context,        df_RowDistributor_1_2_out0)
    val df_SchemaTransform_1_1_1 =
      SchemaTransform_1_1_1(context, df_Reformat_5_1_1)
    val df_Script_1_1   = Script_1_1(context,   df_livyscalaSG1_1_1)
    val df_Reformat_6_1 = Reformat_6_1(context, df_Script_1_1)
    val df_livyscalaSG1_1_1_1 =
      livyscalaSG1_1_1_1.apply(context, df_SetOperation_1_1_1)
    val df_Script_1_1_1        = Script_1_1_1(context,        df_livyscalaSG1_1_1_1)
    val df_Reformat_6_1_1      = Reformat_6_1_1(context,      df_Script_1_1_1)
    val df_Reformat_6          = Reformat_6(context,          df_Script_1)
    val df_SchemaTransform_1_2 = SchemaTransform_1_2(context, df_Reformat_5_2)
    val df_Reformat_6_2        = Reformat_6_2(context,        df_Script_1_2)
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
