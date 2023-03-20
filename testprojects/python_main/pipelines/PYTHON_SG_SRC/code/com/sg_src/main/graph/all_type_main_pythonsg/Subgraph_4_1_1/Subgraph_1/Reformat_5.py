from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from .config import *
from com.sg_src.main.udfs.UDFs import *

def Reformat_5(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        concat(lit(Config.c_sg1_c_string), col("`c-string`")).alias("c1"), 
        col("c_udfs_usage"), 
        col("`c-string`")
    )
