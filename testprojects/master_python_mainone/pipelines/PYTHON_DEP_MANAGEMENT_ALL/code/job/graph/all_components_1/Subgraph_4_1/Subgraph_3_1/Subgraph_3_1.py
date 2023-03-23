from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *
from .config import *

def Subgraph_3_1(spark: SparkSession, config: SubgraphConfig, in0: DataFrame) -> DataFrame:
    Config.update(config)
    df_Reformat_9_1 = Reformat_9_1(spark, in0)
    df_Reformat_9_1 = collectMetrics(
        spark, 
        df_Reformat_9_1, 
        "Subgraph_3_1", 
        "QOVJXBv1pBZqIe3LP3Eh4$$Du2szOfyyKPlku_MZKRsM", 
        "0bcUpFGYhOiCJkMzs-D2x$$b7YuNjpclDteOGia-90jQ"
    )
    df_Subgraph_7_1 = Subgraph_7_1(spark, config.Subgraph_7_1, df_Reformat_9_1)

    return df_Subgraph_7_1
