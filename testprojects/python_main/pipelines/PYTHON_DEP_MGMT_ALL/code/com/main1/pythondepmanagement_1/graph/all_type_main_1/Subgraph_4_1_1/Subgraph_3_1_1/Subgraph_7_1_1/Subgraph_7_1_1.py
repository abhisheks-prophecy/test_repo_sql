from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *

def Subgraph_7_1_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df_OrderBy_4_1_1 = OrderBy_4_1_1(spark, in0)
    df_OrderBy_4_1_1 = collectMetrics(
        spark, 
        df_OrderBy_4_1_1, 
        "Subgraph_7_1_1", 
        "gVKMNC9dxiWr34PMCTYhQ$$ptF-xkATLcvTkdZDOnI-h", 
        "bvGbcYH7KE2BGIgZcjfzI$$8CyyjKHICQghtnB-jbw7R"
    )
    df_Subgraph_8_1_1 = Subgraph_8_1_1(spark, df_OrderBy_4_1_1)

    return df_Subgraph_8_1_1
