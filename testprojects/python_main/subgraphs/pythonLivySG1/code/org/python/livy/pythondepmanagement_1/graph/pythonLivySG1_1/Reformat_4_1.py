from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from com.main1.pythondepmanagement_1.config.ConfigStore import *
from com.main1.pythondepmanagement_1.udfs.UDFs import *

def Reformat_4_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("year"), 
        col("industry_code_ANZSIC"), 
        col("industry_name_ANZSIC"), 
        col("rme_size_grp"), 
        col("variable"), 
        col("value"), 
        col("unit")
    )