package graph

import io.prophecy.libs._
import config.ConfigStore._
import config.Context
import graph.all_types.recursive
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object all_types {

  def apply(
    context: Context,
    in0:     DataFrame,
    in1:     DataFrame,
    in2:     DataFrame
  ): Subgraph4 = {
    val df_src_csv_all_type_no_partition = src_csv_all_type_no_partition(
      context
    )
    Lookup_1(context, df_src_csv_all_type_no_partition)
    val df_Reformat_2       = Reformat_2(context,       in0)
    val df_Filter_1         = Filter_1(context,         df_Reformat_2)
    val df_OrderBy_1        = OrderBy_1(context,        df_Filter_1)
    val df_Limit_1          = Limit_1(context,          df_OrderBy_1)
    val df_WindowFunction_1 = WindowFunction_1(context, df_Limit_1)
    val df_SetOperation_1 =
      SetOperation_1(context, df_WindowFunction_1, df_WindowFunction_1)
    val df_SchemaTransform_1 = SchemaTransform_1(context, df_SetOperation_1)
    val df_Join_1            = Join_1(context,            df_SchemaTransform_1, df_SchemaTransform_1)
    val (df_RowDistributor_1_out0, df_RowDistributor_1_out1) =
      RowDistributor_1(context, df_Join_1)
    val df_Deduplicate_1 = Deduplicate_1(context,   in1)
    val df_Script_1      = Script_1(context,        df_Deduplicate_1)
    val df_recursive     = recursive.apply(context, df_Script_1)
    val df_Reformat_1    = Reformat_1(context,      df_src_csv_all_type_no_partition)
    dest_12345(context, df_Reformat_1)
    val df_Aggregate_1     = Aggregate_1(context,     df_RowDistributor_1_out0)
    val df_OrderBy_2       = OrderBy_2(context,       df_RowDistributor_1_out1)
    val df_FlattenSchema_1 = FlattenSchema_1(context, df_Aggregate_1)
    (df_Reformat_1, df_FlattenSchema_1, df_OrderBy_2, df_recursive)
  }

}
