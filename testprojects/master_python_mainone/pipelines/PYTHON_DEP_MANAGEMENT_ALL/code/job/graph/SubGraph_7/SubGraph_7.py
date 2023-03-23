from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *
from .config import *

def SubGraph_7(spark: SparkSession, config: SubgraphConfig, in0: DataFrame) -> DataFrame:
    Config.update(config)
    df_Reformat_8 = Reformat_8(spark, in0)
    df_Reformat_8 = collectMetrics(
        spark, 
        df_Reformat_8, 
        "SubGraph_7", 
        "ZEY3gafK1VkNqpgbPwW2X$$5mgoHwsKLyaA9V1Hmj45S", 
        "rsfeFdbRoZoIorpsJ7a5K$$5ZjURBrwlP_Z0T5UwSmET"
    )
    df_Script_4 = Script_4(spark, df_Reformat_8)
    df_Script_4 = collectMetrics(
        spark, 
        df_Script_4, 
        "SubGraph_7", 
        "qfOnxjgFsLsGxAWFpfw-t$$9O-rXzWiZL-JvE_8HQAOL", 
        "96SuP_LjpiYnciSLQgTYk$$CwVi_wZEKvvQWXoXoF4jo"
    )

    return df_Script_4
