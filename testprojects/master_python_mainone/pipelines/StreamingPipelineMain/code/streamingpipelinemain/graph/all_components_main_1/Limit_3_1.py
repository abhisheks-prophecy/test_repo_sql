from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from streamingpipelinemain.config.ConfigStore import *
from streamingpipelinemain.udfs.UDFs import *

def Limit_3_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.limit(10)
