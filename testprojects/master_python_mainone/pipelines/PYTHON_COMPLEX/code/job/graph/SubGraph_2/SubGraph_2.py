from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *

def SubGraph_2(spark: SparkSession, in0: DataFrame, in1: DataFrame) -> DataFrame:
    df_Reformat_2 = Reformat_2(spark, in0)
    df_Reformat_2 = collectMetrics(
        spark, 
        df_Reformat_2, 
        "SubGraph_2", 
        "IDtD-6hsl6TU9pjUsXFdD$$3Dkn23DI17sr75QloLw4q", 
        "b9QnM_7xxiPvm3nhgCjih$$vQrEZDFp_589WJXb9tUv2"
    )
    df_Filter_2 = Filter_2(spark, df_Reformat_2)
    df_Filter_2 = collectMetrics(
        spark, 
        df_Filter_2, 
        "SubGraph_2", 
        "pN4rDyXB6AE5MOevEc0ys$$0LpBDaU0G0QC5rVizMxza", 
        "kQCynmvJxlYImFDz6qCHz$$86WwkbOiYHbaWNoxQ-nwa"
    )
    df_SubGraph_3 = SubGraph_3(spark, df_Filter_2)
    df_Reformat_1 = Reformat_1(spark, in1)
    df_Reformat_1 = collectMetrics(
        spark, 
        df_Reformat_1, 
        "SubGraph_2", 
        "PlvwvhuApIIf9JmKUkJBT$$eAxT9l5BytoVJ_TEiei5h", 
        "c3us8wnfQ2DtgC4pQQz5j$$YYCmuu8c3kNpgJvEeaz4r"
    )
    df_Reformat_1.cache().count()
    df_Reformat_1.unpersist()

    return df_SubGraph_3
