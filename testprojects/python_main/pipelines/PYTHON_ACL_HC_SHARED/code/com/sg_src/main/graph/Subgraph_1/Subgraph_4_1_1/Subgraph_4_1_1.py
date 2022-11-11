from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *

def Subgraph_4_1_1(spark: SparkSession, in0: DataFrame, in1: DataFrame, in2: DataFrame) -> DataFrame:
    df_Script_2_1_1 = Script_2_1_1(spark, in0, in1, in2)
    df_Script_2_1_1 = collectMetrics(
        spark, 
        df_Script_2_1_1, 
        "Subgraph_4_1_1", 
        "jXAIarxmRsKCNmKuiGDQW$$xSNUfrf8gREL5gsrZwKbR", 
        "LtJNJ9K2E3T5WSgm96Hxv$$izEyii0Ns2SpwRAqd1aPb"
    )
    df_Subgraph_3_1_1 = Subgraph_3_1_1(spark, df_Script_2_1_1)

    return df_Subgraph_3_1_1
