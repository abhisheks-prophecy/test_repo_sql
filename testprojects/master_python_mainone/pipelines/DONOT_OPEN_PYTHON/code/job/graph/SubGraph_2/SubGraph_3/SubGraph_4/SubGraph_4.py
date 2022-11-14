from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *

def SubGraph_4(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df_Repartition_2 = Repartition_2(spark, in0)
    df_Repartition_2 = collectMetrics(
        spark, 
        df_Repartition_2, 
        "SubGraph_4", 
        "gbwUjx7glIxVQZdTAfM4z$$w_Iwh9RAyFkNhnfQQ-hD_", 
        "liolOST1-BkrvP03XsObu$$g_bANvXrkK1AsrCaEdf7P"
    )
    df_SubGraph_5 = SubGraph_5(spark, df_Repartition_2)

    return df_SubGraph_5
