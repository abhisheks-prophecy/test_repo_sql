from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *

def Subgraph_8_1_1_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df_Subgraph_9_1_1_1 = Subgraph_9_1_1_1(spark, in0)

    return df_Subgraph_9_1_1_1
