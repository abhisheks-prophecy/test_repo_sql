from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Reformat_9_1_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(col("`c-string`"))