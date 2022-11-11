from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *

def Subgraph_4_1(spark: SparkSession, in0: DataFrame, in1: DataFrame, in2: DataFrame) -> DataFrame:
    df_Script_2_1 = Script_2_1(spark, in0, in1, in2)
    df_Script_2_1 = collectMetrics(
        spark, 
        df_Script_2_1, 
        "Subgraph_4_1", 
        "Script_2_1", 
        "nV3cDKSsE7lB7oBPblUN5$$xilYkCwhI-hJdVE2h6xCZ"
    )
    df_Subgraph_3_1 = Subgraph_3_1(spark, df_Script_2_1)

    return df_Subgraph_3_1
