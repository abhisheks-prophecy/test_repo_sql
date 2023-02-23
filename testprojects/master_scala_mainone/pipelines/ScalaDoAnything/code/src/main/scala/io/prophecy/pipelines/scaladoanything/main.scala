package io.prophecy.pipelines.scaladoanything

import io.prophecy.libs._
import io.prophecy.pipelines.scaladoanything.config.ConfigStore._
import io.prophecy.pipelines.scaladoanything.config.Context
import io.prophecy.pipelines.scaladoanything.config._
import io.prophecy.pipelines.scaladoanything.udfs.UDFs._
import io.prophecy.pipelines.scaladoanything.udfs._
import io.prophecy.pipelines.scaladoanything.graph._
import io.prophecy.pipelines.scaladoanything.graph.Subgraph_1
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_src_parquet_all_type_and_partition_withspacehyphens =
      src_parquet_all_type_and_partition_withspacehyphens(context)
    val df_SetOperation_1 = SetOperation_1(
      context,
      df_src_parquet_all_type_and_partition_withspacehyphens,
      df_src_parquet_all_type_and_partition_withspacehyphens
    )
    val df_src_csv_special_char_column_name = src_csv_special_char_column_name(
      context
    )
    val df_Reformat_1 = Reformat_1(
      context,
      df_src_parquet_all_type_and_partition_withspacehyphens
    )
    val df_Filter_1          = Filter_1(context,          df_Reformat_1)
    val df_OrderBy_1         = OrderBy_1(context,         df_Filter_1)
    val df_SchemaTransform_1 = SchemaTransform_1(context, df_SetOperation_1)
    val df_Deduplicate_1     = Deduplicate_1(context,     df_SchemaTransform_1)
    val df_Join_1 = Join_1(context,
                           df_src_csv_special_char_column_name,
                           df_src_csv_special_char_column_name
    )
    val (df_RowDistributor_1_out0, df_RowDistributor_1_out1) =
      RowDistributor_1(context, df_Join_1)
    val df_Repartition_1 = Repartition_1(context, df_RowDistributor_1_out0)
    val df_src_orc_all_type_no_partition = src_orc_all_type_no_partition(
      context
    )
    val df_Limit_1          = Limit_1(context,          df_src_orc_all_type_no_partition)
    val df_OrderBy_2        = OrderBy_2(context,        df_Limit_1)
    val df_WindowFunction_1 = WindowFunction_1(context, df_OrderBy_1)
    val df_Script_1         = Script_1(context,         df_RowDistributor_1_out1)
    val df_FlattenSchema_1  = FlattenSchema_1(context,  df_OrderBy_1)
    val (df_SQLStatement_1_out, df_SQLStatement_1_out1) =
      SQLStatement_1(context, df_Deduplicate_1)
    val df_Aggregate_1 = Aggregate_1(context,      df_OrderBy_1)
    val df_Subgraph_1  = Subgraph_1.apply(context, df_Repartition_1)
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
    spark.conf.set("c_spark1", "spark_value1")
    spark.conf.set("c_spark2", "spark_value2")
    spark.conf
      .set("prophecy.metadata.pipeline.uri",                "pipelines/ScalaDoAnything")
    spark.sparkContext.hadoopConfiguration.set("c_hadoop1", "hadoop_value1")
    spark.sparkContext.hadoopConfiguration.set("c_hadoop2", "hadoop_value2")
    MetricsCollector.start(spark,                           "pipelines/ScalaDoAnything")
    apply(context)
    MetricsCollector.end(spark)
  }

}
