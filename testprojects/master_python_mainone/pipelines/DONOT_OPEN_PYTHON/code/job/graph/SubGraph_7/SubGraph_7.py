from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *

def SubGraph_7(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df_Reformat_8 = Reformat_8(spark, in0)
    df_Reformat_8 = collectMetrics(
        spark, 
        df_Reformat_8, 
        "SubGraph_7", 
        "Reformat_8", 
        "rsfeFdbRoZoIorpsJ7a5K$$5ZjURBrwlP_Z0T5UwSmET"
    )
    df_Script_4 = Script_4(spark, df_Reformat_8)
    df_Script_4 = collectMetrics(
        spark, 
        df_Script_4, 
        "SubGraph_7", 
        "Script_4", 
        "96SuP_LjpiYnciSLQgTYk$$CwVi_wZEKvvQWXoXoF4jo"
    )

    return df_Script_4
