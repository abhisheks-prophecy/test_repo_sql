from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *
from .config import *

def SubGraph_4(spark: SparkSession, config: SubgraphConfig, in0: DataFrame) -> DataFrame:
    Config.update(config)
    df_Repartition_2 = Repartition_2(spark, in0)
    df_SubGraph_5 = SubGraph_5(spark, config.SubGraph_5, df_Repartition_2)

    return df_SubGraph_5
