from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *

def Subgraph_8_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df_OrderBy_5_1 = OrderBy_5_1(spark, in0)
    df_OrderBy_5_1 = collectMetrics(
        spark, 
        df_OrderBy_5_1, 
        "Subgraph_8_1", 
        "TFq3sV3auIsOYrbgy9wvf$$RSW8BD_SOtXvRUvqMAX2C", 
        "YeHZ10E4xzFXqDgqon8Eo$$QjlunbIgFs_ZCkWpxFQFY"
    )
    df_Subgraph_9_1 = Subgraph_9_1(spark, df_OrderBy_5_1)

    return df_Subgraph_9_1
