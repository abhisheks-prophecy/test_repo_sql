package io.prophecy.pipelines.scala_vector_type

import io.prophecy.libs._
import io.prophecy.pipelines.scala_vector_type.config.Context
import io.prophecy.pipelines.scala_vector_type.config._
import io.prophecy.pipelines.scala_vector_type.udfs.UDFs._
import io.prophecy.pipelines.scala_vector_type.udfs._
import io.prophecy.pipelines.scala_vector_type.graph._
import io.prophecy.pipelines.scala_vector_type.graph.Subgraph_1
import io.prophecy.pipelines.scala_vector_type.graph.Subgraph_1.config.{
  Context => Subgraph_1_Context
}
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_normalization_csv_dataset = normalization_csv_dataset(context)
    val df_Script_1                  = Script_1(context,       df_normalization_csv_dataset)
    val df_Filter_1                  = Filter_1(context,       df_Script_1)
    val df_Deduplicate_1             = Deduplicate_1(context,  df_Script_1)
    val df_Limit_1                   = Limit_1(context,        df_Deduplicate_1)
    val df_Join_1                    = Join_1(context,         df_Limit_1,  df_Limit_1)
    val df_SetOperation_1            = SetOperation_1(context, df_Filter_1, df_Filter_1)
    val df_Subgraph_1 = Subgraph_1.apply(
      Subgraph_1_Context(context.spark, context.config.Subgraph_1),
      df_Script_1
    )
    val df_Script_2   = Script_2(context,   df_Script_1)
    val df_Script_3   = Script_3(context,   df_Script_1)
    val df_Reformat_1 = Reformat_1(context, df_Script_1)
    target_scala_vector(context, df_Reformat_1)
    val df_WindowFunction_1  = WindowFunction_1(context,  df_Deduplicate_1)
    val df_OrderBy_1         = OrderBy_1(context,         df_Limit_1)
    val df_Reformat_2        = Reformat_2(context,        df_Script_3)
    val df_Reformat_3        = Reformat_3(context,        df_Reformat_2)
    val df_SchemaTransform_1 = SchemaTransform_1(context, df_OrderBy_1)
    val (df_RowDistributor_1_out0, df_RowDistributor_1_out1) =
      RowDistributor_1(context, df_SchemaTransform_1)
    val df_SQLStatement_1  = SQLStatement_1(context,  df_RowDistributor_1_out0)
    val df_Script_4        = Script_4(context,        df_Reformat_3)
    val df_Aggregate_1     = Aggregate_1(context,     df_Script_1)
    val df_FlattenSchema_1 = FlattenSchema_1(context, df_Script_1)
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
      .set("prophecy.metadata.pipeline.uri", "pipelines/SCALA_VECTOR_TYPE")
    registerUDFs(spark)
    MetricsCollector.start(spark, "pipelines/SCALA_VECTOR_TYPE")
    apply(context)
    MetricsCollector.end(spark)
  }

}
