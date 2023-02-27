package random.prophecy.pipelines.sony_livy_pipe

import io.prophecy.libs._
import random.prophecy.pipelines.sony_livy_pipe.config.ConfigStore._
import random.prophecy.pipelines.sony_livy_pipe.config.Context
import random.prophecy.pipelines.sony_livy_pipe.config._
import random.prophecy.pipelines.sony_livy_pipe.udfs.UDFs._
import random.prophecy.pipelines.sony_livy_pipe.udfs._
import random.prophecy.pipelines.sony_livy_pipe.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_livy_src_csv_2_3_1 = livy_src_csv_2_3_1(context)
    Lookup_2_1(context, df_livy_src_csv_2_3_1)
    val df_livy_src_csv_2_3 = livy_src_csv_2_3(context)
    val df_SQLStatement_2   = SQLStatement_2(context, df_livy_src_csv_2_3)
    Lookup_2(context, df_SQLStatement_2)
    val df_SQLStatement_2_1 = SQLStatement_2_1(context, df_livy_src_csv_2_3)
    val df_Reformat_1_3_3   = Reformat_1_3_3(context,   df_SQLStatement_2_1)
    val df_WindowFunction_2 = WindowFunction_2(context, df_Reformat_1_3_3)
    val df_Filter_3         = Filter_3(context,         df_WindowFunction_2)
    Lookup_2_1_1(context, df_Filter_3)
    val df_livy_src_csv_2_3_2   = livy_src_csv_2_3_2(context)
    val df_Reformat_1_3_3_1     = Reformat_1_3_3_1(context,  df_livy_src_csv_2_3_2)
    val df_SchemaTransform_1    = SchemaTransform_1(context, df_Reformat_1_3_3_1)
    val df_Filter_4             = Filter_4(context,          df_SchemaTransform_1)
    val df_Reformat_1           = Reformat_1(context,        df_Filter_4)
    val df_livy_src_csv_2_3_2_1 = livy_src_csv_2_3_2_1(context)
    val df_Reformat_1_3_3_1_1 =
      Reformat_1_3_3_1_1(context, df_livy_src_csv_2_3_2_1)
    val df_SchemaTransform_1_2 =
      SchemaTransform_1_2(context, df_Reformat_1_3_3_1_1)
    val df_Filter_4_2             = Filter_4_2(context,   df_SchemaTransform_1_2)
    val df_Reformat_1_3           = Reformat_1_3(context, df_Filter_4_2)
    val df_livy_src_csv_2_3_2_1_1 = livy_src_csv_2_3_2_1_1(context)
    val df_Reformat_1_3_3_1_1_1 =
      Reformat_1_3_3_1_1_1(context, df_livy_src_csv_2_3_2_1_1)
    val df_SchemaTransform_1_2_1 =
      SchemaTransform_1_2_1(context, df_Reformat_1_3_3_1_1_1)
    val df_Filter_4_2_1   = Filter_4_2_1(context,   df_SchemaTransform_1_2_1)
    val df_Reformat_1_3_1 = Reformat_1_3_1(context, df_Filter_4_2_1)
    val df_SetOperation_2 =
      SetOperation_2(context, df_Reformat_1, df_Reformat_1_3, df_Reformat_1_3_1)
    val df_Filter_4_1        = Filter_4_1(context,        df_SetOperation_2)
    val df_Reformat_1_2      = Reformat_1_2(context,      df_Filter_4_1)
    val df_Aggregate_1       = Aggregate_1(context,       df_Reformat_1_2)
    val df_Reformat_4        = Reformat_4(context,        df_Aggregate_1)
    val df_SchemaTransform_2 = SchemaTransform_2(context, df_Filter_4_1)
    val df_Filter_5          = Filter_5(context,          df_SchemaTransform_2)
    val df_Reformat_5        = Reformat_5(context,        df_Filter_5)
    val df_Reformat_6        = Reformat_6(context,        df_Reformat_5)
    val df_Filter_4_1_1_1    = Filter_4_1_1_1(context,    df_Reformat_1_3_1)
    val df_Reformat_1_2_1    = Reformat_1_2_1(context,    df_Filter_4_1_1_1)
    val df_SetOperation_2_1 =
      SetOperation_2_1(context, df_Reformat_1, df_Reformat_1_3)
    val df_Filter_4_1_1 = Filter_4_1_1(context, df_SetOperation_2_1)
    val df_Script_3     = Script_3(context,     df_Filter_4_1_1)
    val df_SetOperation_2_1_1 =
      SetOperation_2_1_1(context, df_Script_3, df_Reformat_1_2_1)
    val df_Filter_5_1       = Filter_5_1(context,     df_SetOperation_2_1_1)
    val df_Reformat_5_2     = Reformat_5_2(context,   df_Filter_5_1)
    val df_livy_csv_dataset = livy_csv_dataset(context)
    val df_Reformat_5_2_1   = Reformat_5_2_1(context, df_livy_csv_dataset)
    val df_Reformat_5_2_2   = Reformat_5_2_2(context, df_Reformat_5_2_1)
    val df_SetOperation_1 =
      SetOperation_1(context, df_Reformat_5_2, df_Reformat_5_2_2)
    Script_1(context)
    val df_Reformat_6_2 = Reformat_6_2(context, df_Filter_4_1)
    val df_Aggregate_2  = Aggregate_2(context,  df_SetOperation_1)
    val df_Reformat_7   = Reformat_7(context,   df_Aggregate_2)
    target_dataset(context,       df_Reformat_7)
    target_dataset_1(context,     df_Reformat_4)
    target_dataset_1_1(context,   df_Reformat_6)
    target_dataset_1_1_1(context, df_Reformat_6_2)
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
      .set("prophecy.metadata.pipeline.uri", "pipelines/sony_visa_replica")
    MetricsCollector.start(spark,            "pipelines/sony_visa_replica")
    apply(context)
    MetricsCollector.end(spark)
  }

}
