from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *

def all_components_1(
        spark: SparkSession,
        in0: DataFrame,
        in1: DataFrame,
        in2: DataFrame
) -> (DataFrame, DataFrame, DataFrame):
    df_Source_1_1 = Source_1_1(spark)
    df_Source_1_1 = collectMetrics(
        spark, 
        df_Source_1_1, 
        "all_components_1", 
        "Source_1_1", 
        "rkbS5U7Pr6SlyC02kDJ5x$$VbhJneT-NwucPj1iMlx0_"
    )
    df_Reformat_1_1 = Reformat_1_1(spark, df_Source_1_1)
    df_Reformat_1_1 = collectMetrics(
        spark, 
        df_Reformat_1_1, 
        "all_components_1", 
        "Reformat_1_1", 
        "yFnwetZX3TXNKo_sMI8S1$$YooQz7Bprha_Jc5NF-Wh6"
    )
    df_Reformat_2_1 = Reformat_2_1(spark, in0)
    df_Reformat_2_1 = collectMetrics(
        spark, 
        df_Reformat_2_1, 
        "all_components_1", 
        "Reformat_2_1", 
        "A95bZUwhxhD0ETMULWS_S$$VKAydtwelhCL56GjYKJyv"
    )
    df_Join_1_1 = Join_1_1(spark, df_Reformat_1_1, df_Reformat_2_1)
    df_Join_1_1 = collectMetrics(
        spark, 
        df_Join_1_1, 
        "all_components_1", 
        "Join_1_1", 
        "QyRWz1G38-t77fhBWyzKt$$UZJ7t8F3J3U3gqcWMaxDp"
    )
    df_Limit_1_1 = Limit_1_1(spark, df_Join_1_1)
    df_Limit_1_1 = collectMetrics(
        spark, 
        df_Limit_1_1, 
        "all_components_1", 
        "Limit_1_1", 
        "nP7sF7oQSZgBqaioflvHq$$tbQgPLsLcYu-5593nGkQl"
    )
    df_Filter_1_1 = Filter_1_1(spark, df_Limit_1_1)
    df_Filter_1_1 = collectMetrics(
        spark, 
        df_Filter_1_1, 
        "all_components_1", 
        "Filter_1_1", 
        "-MGY1Yd1cqNCchVzfNwc5$$v5v56Ot3q6F4p4awVix_O"
    )
    df_OrderBy_1_1 = OrderBy_1_1(spark, df_Filter_1_1)
    df_OrderBy_1_1 = collectMetrics(
        spark, 
        df_OrderBy_1_1, 
        "all_components_1", 
        "OrderBy_1_1", 
        "qA5EuXSM1bIh-cjJJPOny$$azDSjH0ZSsaBxA4tQ-Yfg"
    )
    df_Aggregate_1_1 = Aggregate_1_1(spark, df_OrderBy_1_1)
    df_Aggregate_1_1 = collectMetrics(
        spark, 
        df_Aggregate_1_1, 
        "all_components_1", 
        "Aggregate_1_1", 
        "Bm7uppZUSQ2dHVHe5eGJw$$5-pjpSjeRoEDMAdYdtK3W"
    )
    df_SchemaTransform_1_1 = SchemaTransform_1_1(spark, df_Aggregate_1_1)
    df_SchemaTransform_1_1 = collectMetrics(
        spark, 
        df_SchemaTransform_1_1, 
        "all_components_1", 
        "SchemaTransform_1_1", 
        "g-ZXclUijj1xotLoIOhS7$$csJ0w9wRzXQvCjafEKj2v"
    )
    df_Deduplicate_1_1 = Deduplicate_1_1(spark, df_SchemaTransform_1_1)
    df_Deduplicate_1_1 = collectMetrics(
        spark, 
        df_Deduplicate_1_1, 
        "all_components_1", 
        "Deduplicate_1_1", 
        "7An5vQwNfU_HPx8_zA3J8$$z3FT4ySyKKZLMvRU7Vl-3"
    )
    df_OrderBy_3_1 = OrderBy_3_1(spark, in2)
    df_OrderBy_3_1 = collectMetrics(
        spark, 
        df_OrderBy_3_1, 
        "all_components_1", 
        "OrderBy_3_1", 
        "bEfYRYQwKSD8As9A7utws$$32_IJoNwkF6SQMTBycQ1q"
    )
    df_Repartition_1_1 = Repartition_1_1(spark, df_Limit_1_1)
    df_Repartition_1_1 = collectMetrics(
        spark, 
        df_Repartition_1_1, 
        "all_components_1", 
        "Repartition_1_1", 
        "1iCEBsfWXHTJ9jositDqz$$qTCaUw_LN7nGiGhxFMNV2"
    )
    df_RowDistributor_1_1_out0, df_RowDistributor_1_1_out1 = RowDistributor_1_1(spark, df_Repartition_1_1)
    df_RowDistributor_1_1_out0 = collectMetrics(
        spark, 
        df_RowDistributor_1_1_out0, 
        "all_components_1", 
        "RowDistributor_1_1", 
        "out0$$KAR5rZnPlaaIKt-CPvSIc"
    )
    df_RowDistributor_1_1_out1 = collectMetrics(
        spark, 
        df_RowDistributor_1_1_out1, 
        "all_components_1", 
        "RowDistributor_1_1", 
        "out1$$QqAmPxp0EO8-Cv_80iAO_"
    )
    df_Deduplicate_2_1 = Deduplicate_2_1(spark, df_SchemaTransform_1_1)
    df_Deduplicate_2_1 = collectMetrics(
        spark, 
        df_Deduplicate_2_1, 
        "all_components_1", 
        "Deduplicate_2_1", 
        "47y9e-7QriD9beHqb8gyU$$NHw_vpeikma0DX1dc3kFA"
    )
    df_Deduplicate_3_1 = Deduplicate_3_1(spark, df_OrderBy_3_1)
    df_Deduplicate_3_1 = collectMetrics(
        spark, 
        df_Deduplicate_3_1, 
        "all_components_1", 
        "Deduplicate_3_1", 
        "IlNnoT4BP3VPlgs0AfEg9$$89FD7tLNE1XoxVvbNCFVi"
    )
    df_SetOperation_1_1 = SetOperation_1_1(spark, df_Deduplicate_1_1, df_Deduplicate_2_1)
    df_SetOperation_1_1 = collectMetrics(
        spark, 
        df_SetOperation_1_1, 
        "all_components_1", 
        "SetOperation_1_1", 
        "ovdFS3IdsuzRQWQYZOp5h$$fNpDDD-GUhDW7LE4Lm8iN"
    )
    df_WindowFunction_1_1 = WindowFunction_1_1(spark, df_SetOperation_1_1)
    df_WindowFunction_1_1 = collectMetrics(
        spark, 
        df_WindowFunction_1_1, 
        "all_components_1", 
        "WindowFunction_1_1", 
        "yJXTo97VxtNRK3OkvwrB3$$Vejcp5-B6jEaNe7zG1DPu"
    )
    df_Script_1_1 = Script_1_1(spark, df_WindowFunction_1_1)
    df_Script_1_1 = collectMetrics(
        spark, 
        df_Script_1_1, 
        "all_components_1", 
        "Script_1_1", 
        "EIrSKdmrWpBzyJYhWG4Yn$$99UY_onljEJiRUMBfxOOu"
    )
    df_Subgraph_4_1 = Subgraph_4_1(spark, df_RowDistributor_1_1_out0, df_RowDistributor_1_1_out1, df_Script_1_1)
    df_Reformat_8_1 = Reformat_8_1(spark, in1)
    df_Reformat_8_1 = collectMetrics(
        spark, 
        df_Reformat_8_1, 
        "all_components_1", 
        "Reformat_8_1", 
        "9xoGUzXd9Ske-evg8CBsi$$-tyLl_DlauPtj7Pr5jCA5"
    )
    df_Limit_3_1 = Limit_3_1(spark, df_Reformat_8_1)
    df_Limit_3_1 = collectMetrics(
        spark, 
        df_Limit_3_1, 
        "all_components_1", 
        "Limit_3_1", 
        "4eY-bznkPfHUJ2QdTuepW$$1ZQU6D0FcKVHHKhP8FRhS"
    )

    return df_Subgraph_4_1, df_Limit_3_1, df_Deduplicate_3_1
