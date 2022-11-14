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
        "ELgVQDpImJdw_GKDMvjcG$$Gb93GVUWL1V1owu3NEV_W", 
        "rkbS5U7Pr6SlyC02kDJ5x$$VbhJneT-NwucPj1iMlx0_"
    )
    df_Reformat_1_1 = Reformat_1_1(spark, df_Source_1_1)
    df_Reformat_1_1 = collectMetrics(
        spark, 
        df_Reformat_1_1, 
        "all_components_1", 
        "XBhDtVydMAUTLG5EQ6MEm$$BisyZu35brxmoDX9LSFTe", 
        "yFnwetZX3TXNKo_sMI8S1$$YooQz7Bprha_Jc5NF-Wh6"
    )
    df_Reformat_2_1 = Reformat_2_1(spark, in0)
    df_Reformat_2_1 = collectMetrics(
        spark, 
        df_Reformat_2_1, 
        "all_components_1", 
        "RpoRoHlutaKtGeUELcHy5$$XiCCHF8D1wYi_Q1L8FGJr", 
        "A95bZUwhxhD0ETMULWS_S$$VKAydtwelhCL56GjYKJyv"
    )
    df_Join_1_1 = Join_1_1(spark, df_Reformat_1_1, df_Reformat_2_1)
    df_Join_1_1 = collectMetrics(
        spark, 
        df_Join_1_1, 
        "all_components_1", 
        "_-1Q8lNZOBPy1ffrNMMuS$$FFK1nOENLtFynnh7sC3p8", 
        "QyRWz1G38-t77fhBWyzKt$$UZJ7t8F3J3U3gqcWMaxDp"
    )
    df_Limit_1_1 = Limit_1_1(spark, df_Join_1_1)
    df_Limit_1_1 = collectMetrics(
        spark, 
        df_Limit_1_1, 
        "all_components_1", 
        "pftQ_HrWF4cDKIfHrWUtc$$n6OB0D69OpJk2uRLy6M71", 
        "nP7sF7oQSZgBqaioflvHq$$tbQgPLsLcYu-5593nGkQl"
    )
    df_Filter_1_1 = Filter_1_1(spark, df_Limit_1_1)
    df_Filter_1_1 = collectMetrics(
        spark, 
        df_Filter_1_1, 
        "all_components_1", 
        "sralgEouOoZeTgcGqtelA$$aOu5GBq_Rfq6knyvn8wcf", 
        "-MGY1Yd1cqNCchVzfNwc5$$v5v56Ot3q6F4p4awVix_O"
    )
    df_OrderBy_1_1 = OrderBy_1_1(spark, df_Filter_1_1)
    df_OrderBy_1_1 = collectMetrics(
        spark, 
        df_OrderBy_1_1, 
        "all_components_1", 
        "Vk4cwbVe0ZZubSAbtcK9O$$HNv5yCIVRV2xPc77DWxOS", 
        "qA5EuXSM1bIh-cjJJPOny$$azDSjH0ZSsaBxA4tQ-Yfg"
    )
    df_Aggregate_1_1 = Aggregate_1_1(spark, df_OrderBy_1_1)
    df_Aggregate_1_1 = collectMetrics(
        spark, 
        df_Aggregate_1_1, 
        "all_components_1", 
        "mMfQX7Os6x6dAwjqTeFFl$$LnUiKJEPpBq_aDe_Gz3fT", 
        "Bm7uppZUSQ2dHVHe5eGJw$$5-pjpSjeRoEDMAdYdtK3W"
    )
    df_SchemaTransform_1_1 = SchemaTransform_1_1(spark, df_Aggregate_1_1)
    df_SchemaTransform_1_1 = collectMetrics(
        spark, 
        df_SchemaTransform_1_1, 
        "all_components_1", 
        "5t4mAZG1a1KU9NzmL--O2$$Z6b4aVHk9RPYia-XoGxys", 
        "g-ZXclUijj1xotLoIOhS7$$csJ0w9wRzXQvCjafEKj2v"
    )
    df_Deduplicate_1_1 = Deduplicate_1_1(spark, df_SchemaTransform_1_1)
    df_Deduplicate_1_1 = collectMetrics(
        spark, 
        df_Deduplicate_1_1, 
        "all_components_1", 
        "_mgJWXDdlj19eZrv3uaPx$$9D__orP7bVD3GbNrpxukG", 
        "7An5vQwNfU_HPx8_zA3J8$$z3FT4ySyKKZLMvRU7Vl-3"
    )
    df_OrderBy_3_1 = OrderBy_3_1(spark, in2)
    df_OrderBy_3_1 = collectMetrics(
        spark, 
        df_OrderBy_3_1, 
        "all_components_1", 
        "gsdAb_Vp8L3S95LLDYafc$$pjTm28y3YIiEKz6edPBV1", 
        "bEfYRYQwKSD8As9A7utws$$32_IJoNwkF6SQMTBycQ1q"
    )
    df_Repartition_1_1 = Repartition_1_1(spark, df_Limit_1_1)
    df_Repartition_1_1 = collectMetrics(
        spark, 
        df_Repartition_1_1, 
        "all_components_1", 
        "75Ld1J0SHPA5EvT1nwWiw$$G3SEdQiewp7W7YSXDrf9M", 
        "1iCEBsfWXHTJ9jositDqz$$qTCaUw_LN7nGiGhxFMNV2"
    )
    df_RowDistributor_1_1_out0, df_RowDistributor_1_1_out1 = RowDistributor_1_1(spark, df_Repartition_1_1)
    df_RowDistributor_1_1_out0 = collectMetrics(
        spark, 
        df_RowDistributor_1_1_out0, 
        "all_components_1", 
        "Zzlr5YLzPAvX1dMNPSdGa$$heRRrqR_-0oT5afRxvHvb", 
        "out0$$KAR5rZnPlaaIKt-CPvSIc"
    )
    df_RowDistributor_1_1_out1 = collectMetrics(
        spark, 
        df_RowDistributor_1_1_out1, 
        "all_components_1", 
        "Zzlr5YLzPAvX1dMNPSdGa$$heRRrqR_-0oT5afRxvHvb", 
        "out1$$QqAmPxp0EO8-Cv_80iAO_"
    )
    df_Deduplicate_2_1 = Deduplicate_2_1(spark, df_SchemaTransform_1_1)
    df_Deduplicate_2_1 = collectMetrics(
        spark, 
        df_Deduplicate_2_1, 
        "all_components_1", 
        "u5d_T01DV-YajkBUTRbQm$$XnysI-8XVjqq93tsQWGnS", 
        "47y9e-7QriD9beHqb8gyU$$NHw_vpeikma0DX1dc3kFA"
    )
    df_Deduplicate_3_1 = Deduplicate_3_1(spark, df_OrderBy_3_1)
    df_Deduplicate_3_1 = collectMetrics(
        spark, 
        df_Deduplicate_3_1, 
        "all_components_1", 
        "HlhnkMa6WMK4gQux41IFv$$jPzy4wgrAKJVk577rLnT9", 
        "IlNnoT4BP3VPlgs0AfEg9$$89FD7tLNE1XoxVvbNCFVi"
    )
    df_SetOperation_1_1 = SetOperation_1_1(spark, df_Deduplicate_1_1, df_Deduplicate_2_1)
    df_SetOperation_1_1 = collectMetrics(
        spark, 
        df_SetOperation_1_1, 
        "all_components_1", 
        "7nj_B1CJDExWSHwmpCbbQ$$98lfBguNnobPgkd-Bk__7", 
        "ovdFS3IdsuzRQWQYZOp5h$$fNpDDD-GUhDW7LE4Lm8iN"
    )
    df_WindowFunction_1_1 = WindowFunction_1_1(spark, df_SetOperation_1_1)
    df_WindowFunction_1_1 = collectMetrics(
        spark, 
        df_WindowFunction_1_1, 
        "all_components_1", 
        "IDq8EyuycHj1DVVcrUJ1B$$976KnqnhsHi0BSRvn_KAz", 
        "yJXTo97VxtNRK3OkvwrB3$$Vejcp5-B6jEaNe7zG1DPu"
    )
    df_Script_1_1 = Script_1_1(spark, df_WindowFunction_1_1)
    df_Script_1_1 = collectMetrics(
        spark, 
        df_Script_1_1, 
        "all_components_1", 
        "10Z1_u7A7Pej444vqthCn$$4TkFmoB26RV0npfpSFqDH", 
        "EIrSKdmrWpBzyJYhWG4Yn$$99UY_onljEJiRUMBfxOOu"
    )
    df_Subgraph_4_1 = Subgraph_4_1(spark, df_RowDistributor_1_1_out0, df_RowDistributor_1_1_out1, df_Script_1_1)
    df_Reformat_8_1 = Reformat_8_1(spark, in1)
    df_Reformat_8_1 = collectMetrics(
        spark, 
        df_Reformat_8_1, 
        "all_components_1", 
        "LQj_n-h8IUBxiGYHRMEx1$$7-65G9iuMEocMRehPtT1v", 
        "9xoGUzXd9Ske-evg8CBsi$$-tyLl_DlauPtj7Pr5jCA5"
    )
    df_Limit_3_1 = Limit_3_1(spark, df_Reformat_8_1)
    df_Limit_3_1 = collectMetrics(
        spark, 
        df_Limit_3_1, 
        "all_components_1", 
        "e-knXo6KZCOovflSydG6F$$gknNFgX771-dKOFM1qKfi", 
        "4eY-bznkPfHUJ2QdTuepW$$1ZQU6D0FcKVHHKhP8FRhS"
    )

    return df_Subgraph_4_1, df_Limit_3_1, df_Deduplicate_3_1
