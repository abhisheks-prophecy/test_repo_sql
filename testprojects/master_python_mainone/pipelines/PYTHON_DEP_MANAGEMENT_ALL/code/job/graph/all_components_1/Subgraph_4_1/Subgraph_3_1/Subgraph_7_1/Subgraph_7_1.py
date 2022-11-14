from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *

def Subgraph_7_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df_OrderBy_4_1 = OrderBy_4_1(spark, in0)
    df_OrderBy_4_1 = collectMetrics(
        spark, 
        df_OrderBy_4_1, 
        "Subgraph_7_1", 
        "xaB9Mv6rlDaXUbRW28Ir2$$w1b-NcTzrDeIKUSMULmYm", 
        "keK89GSJR8p2jJsTsVuP3$$L_U6lDYJ7JxhGSdhTKtCZ"
    )
    df_Subgraph_8_1 = Subgraph_8_1(spark, df_OrderBy_4_1)

    return df_Subgraph_8_1
