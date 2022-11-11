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
        "PlvwvhuApIIf9JmKUkJBT$$eAxT9l5BytoVJ_TEiei5h", 
        "c3us8wnfQ2DtgC4pQQz5j$$YYCmuu8c3kNpgJvEeaz4r"
    )
    df_Script_5 = Script_5(spark, df_Reformat_1)
    df_Script_5 = collectMetrics(
        spark, 
        df_Script_5, 
        "SubGraph_2", 
        "HOZfyguXrMBHHg8Wqxz5U$$zOwbfiEpCyHT-FnPJLBXg", 
        "Yq9SIZO0kq9sKjKPMERCW$$M1Cx-q7cMu7qMTuZjh71S"
    )
    df_Filter_4 = Filter_4(spark, df_Script_5)
    df_Filter_4 = collectMetrics(
        spark, 
        df_Filter_4, 
        "SubGraph_2", 
        "yG1aPKdrqanhRR0VpDBXv$$Bf9-WIgnZPWXeSMBa-E_l", 
        "9LkgYk4uuAeM0wiqe4x2p$$J8Q7YulGJBzK25od2OB7m"
    )
    df_Filter_4.cache().count()
    df_Filter_4.unpersist()
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
    df_custom_xlsx_py_1 = custom_xlsx_py_1(spark)
    df_custom_xlsx_py_1 = collectMetrics(
        spark, 
        df_custom_xlsx_py_1, 
        "SubGraph_2", 
        "z9czZlAwCW268RRLNobJB$$4BoDztVhKdwHbTdbWwMm1", 
        "OKNpZCciglbiZOKCZAGRE$$CWwkKQMQcViqrnUSaeJ2Z"
    )
    df_Reformat_10_1 = Reformat_10_1(spark, df_custom_xlsx_py_1)
    df_Reformat_10_1 = collectMetrics(
        spark, 
        df_Reformat_10_1, 
        "SubGraph_2", 
        "Myut1NAouU399u-oNA-Ad$$TqATGrkxaUl1FjGC9-xZP", 
        "kR962bQT-V0t3TrnF3zV4$$u3ADdnEYiZYWOsumdS3xX"
    )
    df_Reformat_10_1.cache().count()
    df_Reformat_10_1.unpersist()
    df_SubGraph_3 = SubGraph_3(spark, df_Filter_2)

    return df_SubGraph_3
