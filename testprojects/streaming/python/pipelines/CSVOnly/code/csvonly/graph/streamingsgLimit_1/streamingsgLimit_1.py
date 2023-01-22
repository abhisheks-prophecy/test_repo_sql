from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *

def streamingsgLimit_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df_Limit_2 = Limit_2(spark, in0)

    return df_Limit_2
