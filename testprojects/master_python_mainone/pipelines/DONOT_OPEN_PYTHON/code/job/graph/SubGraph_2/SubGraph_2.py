from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *

def SubGraph_2(spark: SparkSession, in0: DataFrame, in1: DataFrame) -> DataFrame:
    df_Reformat_1 = Reformat_1(spark, in1)
    df_Reformat_1 = collectMetrics(
        spark, 
        df_Reformat_1, 
        "SubGraph_2", 
        "Reformat_1", 
        "c3us8wnfQ2DtgC4pQQz5j$$YYCmuu8c3kNpgJvEeaz4r"
    )
    df_Script_5 = Script_5(spark, df_Reformat_1)
    df_Script_5 = collectMetrics(
        spark, 
        df_Script_5, 
        "SubGraph_2", 
        "Script_5", 
        "Yq9SIZO0kq9sKjKPMERCW$$M1Cx-q7cMu7qMTuZjh71S"
    )
    df_Filter_4 = Filter_4(spark, df_Script_5)
    df_Filter_4 = collectMetrics(
        spark, 
        df_Filter_4, 
        "SubGraph_2", 
        "Filter_4", 
        "9LkgYk4uuAeM0wiqe4x2p$$J8Q7YulGJBzK25od2OB7m"
    )
    df_Filter_4.cache().count()
    df_Filter_4.unpersist()
    df_Reformat_2 = Reformat_2(spark, in0)
    df_Reformat_2 = collectMetrics(
        spark, 
        df_Reformat_2, 
        "SubGraph_2", 
        "Reformat_2", 
        "b9QnM_7xxiPvm3nhgCjih$$vQrEZDFp_589WJXb9tUv2"
    )
    df_Filter_2 = Filter_2(spark, df_Reformat_2)
    df_Filter_2 = collectMetrics(
        spark, 
        df_Filter_2, 
        "SubGraph_2", 
        "Filter_2", 
        "kQCynmvJxlYImFDz6qCHz$$86WwkbOiYHbaWNoxQ-nwa"
    )
    df_custom_xlsx_py_1 = custom_xlsx_py_1(spark)
    df_custom_xlsx_py_1 = collectMetrics(
        spark, 
        df_custom_xlsx_py_1, 
        "SubGraph_2", 
        "custom_xlsx_py_1", 
        "OKNpZCciglbiZOKCZAGRE$$CWwkKQMQcViqrnUSaeJ2Z"
    )
    df_Reformat_10_1 = Reformat_10_1(spark, df_custom_xlsx_py_1)
    df_Reformat_10_1 = collectMetrics(
        spark, 
        df_Reformat_10_1, 
        "SubGraph_2", 
        "Reformat_10_1", 
        "kR962bQT-V0t3TrnF3zV4$$u3ADdnEYiZYWOsumdS3xX"
    )
    df_Reformat_10_1.cache().count()
    df_Reformat_10_1.unpersist()
    df_SubGraph_3 = SubGraph_3(spark, df_Filter_2)

    return df_SubGraph_3
