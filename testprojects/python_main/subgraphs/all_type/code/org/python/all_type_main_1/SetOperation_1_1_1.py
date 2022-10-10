from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pythongenericuse.config.ConfigStore import *
from pythongenericuse.udfs.UDFs import *

def SetOperation_1_1_1(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0.unionAll(in1)
