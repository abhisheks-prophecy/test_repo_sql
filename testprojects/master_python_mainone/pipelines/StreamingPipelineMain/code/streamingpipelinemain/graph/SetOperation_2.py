from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from streamingpipelinemain.config.ConfigStore import *
from streamingpipelinemain.udfs.UDFs import *

def SetOperation_2(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0.unionAll(in1)