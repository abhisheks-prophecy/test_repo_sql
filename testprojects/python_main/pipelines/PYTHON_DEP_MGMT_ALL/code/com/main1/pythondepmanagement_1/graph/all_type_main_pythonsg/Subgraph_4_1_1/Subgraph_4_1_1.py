from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *
from .config import *

def Subgraph_4_1_1(
        spark: SparkSession,
        config: SubgraphConfig,
        in0: DataFrame,
        in1: DataFrame,
        in2: DataFrame
) -> DataFrame:
    Config.update(config)
    df_Script_2_1_1 = Script_2_1_1(spark, in0, in1, in2)
    df_Script_2_1_1 = collectMetrics(
        spark, 
        df_Script_2_1_1, 
        "Subgraph_4_1_1", 
        "jXAIarxmRsKCNmKuiGDQW$$PTwrvN7JP44hstMojqHr0", 
        "LtJNJ9K2E3T5WSgm96Hxv$$q9qObYoorGVekbmcqaNgB"
    )
    df_Subgraph_3_1_1 = Subgraph_3_1_1(spark, config.Subgraph_3_1_1, df_Script_2_1_1)
    df_Reformat_2 = Reformat_2(spark, df_Script_2_1_1)
    df_Reformat_2 = collectMetrics(
        spark, 
        df_Reformat_2, 
        "Subgraph_4_1_1", 
        "uO641onqNbuvrG69asLC4$$4-nqDLwvHFxea_doDf_G1", 
        "Uis7YXQybZGwQKxiIeor7$$AWN05_sCX5008KMd-Uxxj"
    )
    df_Reformat_2.cache().count()
    df_Reformat_2.unpersist()

    return df_Subgraph_3_1_1
