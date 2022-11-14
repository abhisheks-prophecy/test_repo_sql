from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *

def MultiPort(
        spark: SparkSession,
        in0: DataFrame,
        in1: DataFrame,
        in2: DataFrame,
        in3: DataFrame
) -> (DataFrame, DataFrame, DataFrame, DataFrame):
    df_OrderBy_2 = OrderBy_2(spark, in2)
    df_Limit_2 = Limit_2(spark, in3)
    df_Filter_3 = Filter_3(spark, in1)
    df_Reformat_7 = Reformat_7(spark, in0)

    return df_Reformat_7, df_Filter_3, df_OrderBy_2, df_Limit_2
