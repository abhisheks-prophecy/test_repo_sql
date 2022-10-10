from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *

def Subgraph_8_1_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df_OrderBy_5_1_1 = OrderBy_5_1_1(spark, in0)
    df_OrderBy_5_1_1 = collectMetrics(
        spark, 
        df_OrderBy_5_1_1, 
        "Subgraph_8_1_1", 
        "2OW_e2OWirNT8Iv2PeGCl$$1yaCAYHFzAff3NwP1azk7", 
        "JDJyA499iZD_5Wi2NBhj5$$_zPj4i7XsII07qY5e-r4J"
    )
    df_Subgraph_9_1_1 = Subgraph_9_1_1(spark, df_OrderBy_5_1_1)

    return df_Subgraph_9_1_1
