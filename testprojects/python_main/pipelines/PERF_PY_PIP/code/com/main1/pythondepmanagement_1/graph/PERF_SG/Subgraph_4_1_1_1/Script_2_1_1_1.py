from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from com.main1.pythondepmanagement_1.config.ConfigStore import *
from com.main1.pythondepmanagement_1.udfs.UDFs import *

def Script_2_1_1_1(spark: SparkSession, in0: DataFrame, in1: DataFrame, in2: DataFrame) -> DataFrame:
    out1 = in0.select("c_concat_col")
    out2 = in1.select("c_concat_col")
    out3 = in2.select("c_concat_col")
    out0 = out1.union(out2).union(out3)

    return out0
