from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *

def Subgraph_3_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df_Reformat_9_1 = Reformat_9_1(spark, in0)
    df_Reformat_9_1 = collectMetrics(
        spark, 
        df_Reformat_9_1, 
        "Subgraph_3_1", 
        "Reformat_9_1", 
        "0bcUpFGYhOiCJkMzs-D2x$$b7YuNjpclDteOGia-90jQ"
    )
    df_Subgraph_7_1 = Subgraph_7_1(spark, df_Reformat_9_1)

    return df_Subgraph_7_1
