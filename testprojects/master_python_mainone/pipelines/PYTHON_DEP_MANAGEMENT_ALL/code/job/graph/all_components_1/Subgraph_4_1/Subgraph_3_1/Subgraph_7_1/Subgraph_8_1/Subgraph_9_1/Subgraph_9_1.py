from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *
from .config import *

def Subgraph_9_1(spark: SparkSession, config: SubgraphConfig, in0: DataFrame) -> DataFrame:
    Config.update(config)
    df_Reformat_10_1 = Reformat_10_1(spark, in0)
    df_Reformat_10_1 = collectMetrics(
        spark, 
        df_Reformat_10_1, 
        "Subgraph_9_1", 
        "-z3MKlPPEpJi2WxeoOZte$$0u_6OAxwVGLoX6amtRjW4", 
        "dLw7bCjWBn39vHDz2NxPv$$AknbNwZqV6x8jdw6lZyAi"
    )

    return df_Reformat_10_1
