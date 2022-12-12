from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *

def SubGraph_5(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df_OrderBy_2 = OrderBy_2(spark, in0)
    df_SubGraph_6 = SubGraph_6(spark, df_OrderBy_2)

    return df_SubGraph_6