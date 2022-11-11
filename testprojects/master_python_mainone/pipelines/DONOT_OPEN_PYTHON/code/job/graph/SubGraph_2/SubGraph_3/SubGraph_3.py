from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *

def SubGraph_3(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df_Join_2 = Join_2(spark, in0, in0)
    df_Join_2 = collectMetrics(spark, df_Join_2, "SubGraph_3", "Join_2", "32cuP7tVD5gxg3q1xwLwC$$XDO7RspnOmKsZb4TKLUHj")
    df_SubGraph_4 = SubGraph_4(spark, df_Join_2)

    return df_SubGraph_4
