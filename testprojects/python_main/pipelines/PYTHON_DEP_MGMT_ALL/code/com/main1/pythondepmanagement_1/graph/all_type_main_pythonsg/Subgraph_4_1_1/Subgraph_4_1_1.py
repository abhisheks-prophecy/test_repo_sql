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
    df_very_complex_dataset = very_complex_dataset(spark)
    df_very_complex_dataset = collectMetrics(
        spark, 
        df_very_complex_dataset, 
        "Subgraph_4_1_1", 
        "kJhGIl4913igbop3zHWgB$$gMHLSb-ebnBQgdV61dJnB", 
        "Hva5MZZ-7DL6Hbo60kTk0$$GW2uDTtfhG9fb3gV9s_uT"
    )
    df_FlattenSchema_1 = FlattenSchema_1(spark, df_very_complex_dataset)
    df_FlattenSchema_1 = collectMetrics(
        spark, 
        df_FlattenSchema_1, 
        "Subgraph_4_1_1", 
        "QezeGN_HIODRv516024eh$$3ogoWucaJnV0mTYKvRaVu", 
        "RLwj_FQ5FOnl1S-jW60s8$$oiNOu3x0HVVeGxsMgd0tq"
    )
    df_Reformat_2 = Reformat_2(spark, df_Script_2_1_1)
    df_Reformat_2 = collectMetrics(
        spark, 
        df_Reformat_2, 
        "Subgraph_4_1_1", 
        "uO641onqNbuvrG69asLC4$$4-nqDLwvHFxea_doDf_G1", 
        "Uis7YXQybZGwQKxiIeor7$$AWN05_sCX5008KMd-Uxxj"
    )
    df_Subgraph_1 = Subgraph_1(spark, config.Subgraph_1, df_Reformat_2)
    df_Subgraph_1.cache().count()
    df_Subgraph_1.unpersist()
    df_Reformat_9 = Reformat_9(spark, df_FlattenSchema_1)
    df_Reformat_9 = collectMetrics(
        spark, 
        df_Reformat_9, 
        "Subgraph_4_1_1", 
        "tnvPUyKHAzjnDjUQOPEYw$$ZDyUuRyU3dUiVpAkVNp0o", 
        "VZNjHkBFcJv_rpC8K1brK$$7d9r_zhp1YHCE2Q9y3A9d"
    )
    df_Reformat_9.cache().count()
    df_Reformat_9.unpersist()
    df_Reformat_7 = Reformat_7(spark, df_very_complex_dataset)
    df_Reformat_7 = collectMetrics(
        spark, 
        df_Reformat_7, 
        "Subgraph_4_1_1", 
        "hGgXKTfmsL-h-QNQYA2Ro$$qx-eyMSq1cJTGfV6GYG6q", 
        "7BTxigjNe4Wk5dZzGhEtQ$$b0xUrBvuCdtPy5JCCgwYm"
    )
    df_Subgraph_3_1_1 = Subgraph_3_1_1(spark, config.Subgraph_3_1_1, df_Script_2_1_1)
    df_Reformat_8 = Reformat_8(spark, df_Reformat_7)
    df_Reformat_8 = collectMetrics(
        spark, 
        df_Reformat_8, 
        "Subgraph_4_1_1", 
        "vLDTgHevVHKQUJu130PVx$$oDdPVf675iHeg7OzZ5AKD", 
        "NkoKcxUjyneSVM3ylTllv$$ETJSmd6tQWMyB8QF_Wk02"
    )
    df_Reformat_8.cache().count()
    df_Reformat_8.unpersist()

    return df_Subgraph_3_1_1
