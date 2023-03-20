from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *
from .config import *

def Subgraph_1(spark: SparkSession, config: SubgraphConfig, in0: DataFrame) -> DataFrame:
    Config.update(config)
    df_Reformat_5 = Reformat_5(spark, in0)
    df_Reformat_5 = collectMetrics(
        spark, 
        df_Reformat_5, 
        "Subgraph_1", 
        "23JD6gZAyxnx3fvhHQ0B1$$DzUNoLHbFkWQKsvqs-t1S", 
        "JZGkRLYuhU6TGyliqZw2r$$PZwmDl28d1SJq9jvVOZ4R"
    )

    return df_Reformat_5
