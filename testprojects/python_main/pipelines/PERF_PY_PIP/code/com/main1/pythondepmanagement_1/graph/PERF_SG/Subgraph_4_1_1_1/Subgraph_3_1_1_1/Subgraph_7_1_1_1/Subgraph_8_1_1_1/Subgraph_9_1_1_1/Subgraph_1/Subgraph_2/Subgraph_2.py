from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *

def Subgraph_2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df_Subgraph_3 = Subgraph_3(spark, in0)

    return df_Subgraph_3
