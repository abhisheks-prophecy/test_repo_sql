from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from com.sg_src.main.config.ConfigStore import *
from com.sg_src.main.udfs.UDFs import *

def Reformat_10_1_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(col("`c-string`"))
