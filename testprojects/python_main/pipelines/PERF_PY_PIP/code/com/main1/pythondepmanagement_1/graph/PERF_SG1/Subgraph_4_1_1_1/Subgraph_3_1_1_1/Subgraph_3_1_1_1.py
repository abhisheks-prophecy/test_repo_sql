from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *

def Subgraph_3_1_1_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df_Reformat_1 = Reformat_1(spark, in0)
    df_Reformat_1 = collectMetrics(
        spark, 
        df_Reformat_1, 
        "Subgraph_3_1_1_1", 
        "k6E7HAGgATr_UD2_cDrbk$$gfw6NabVWQpkkG78ruQS6", 
        "Y-7sh5Qo6A4GhPU--2zvX$$Meirl1eRBRSkSsbELWCPt"
    )
    df_Subgraph_7_1_1_1 = Subgraph_7_1_1_1(spark, df_Reformat_1)

    return df_Subgraph_7_1_1_1
