package graph

import io.prophecy.libs._
import graph.all_types.recursive
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object all_types {

  def apply(
    spark: SparkSession,
    in0:   DataFrame,
    in1:   DataFrame,
    in2:   DataFrame
  ): Subgraph4 = {
    val df_src_csv_all_type_no_partition = src_csv_all_type_no_partition(spark)
    Lookup_1(spark, df_src_csv_all_type_no_partition)
    val df_Reformat_2       = Reformat_2(spark,       in0)
    val df_Filter_1         = Filter_1(spark,         df_Reformat_2)
    val df_OrderBy_1        = OrderBy_1(spark,        df_Filter_1)
    val df_Limit_1          = Limit_1(spark,          df_OrderBy_1)
    val df_WindowFunction_1 = WindowFunction_1(spark, df_Limit_1)
    val df_SetOperation_1 =
      SetOperation_1(spark, df_WindowFunction_1, df_WindowFunction_1)
    val df_SchemaTransform_1 = SchemaTransform_1(spark, df_SetOperation_1)
    val df_Join_1            = Join_1(spark,            df_SchemaTransform_1, df_SchemaTransform_1)
    val (df_RowDistributor_1_out0, df_RowDistributor_1_out1) =
      RowDistributor_1(spark, df_Join_1)
    val df_Deduplicate_1 = Deduplicate_1(spark,   in1)
    val df_Script_1      = Script_1(spark,        df_Deduplicate_1)
    val df_recursive     = recursive.apply(spark, df_Script_1)
    val df_Reformat_1    = Reformat_1(spark,      df_src_csv_all_type_no_partition)
    dest_12345(spark, df_Reformat_1)
    val df_Aggregate_1     = Aggregate_1(spark,     df_RowDistributor_1_out0)
    val df_OrderBy_2       = OrderBy_2(spark,       df_RowDistributor_1_out1)
    val df_FlattenSchema_1 = FlattenSchema_1(spark, df_Aggregate_1)
    (df_Reformat_1, df_FlattenSchema_1, df_OrderBy_2, df_recursive)
  }

}
