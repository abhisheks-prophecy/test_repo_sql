from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *

def Subgraph_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df_Subgraph_2 = Subgraph_2(spark, in0)

    return df_Subgraph_2
