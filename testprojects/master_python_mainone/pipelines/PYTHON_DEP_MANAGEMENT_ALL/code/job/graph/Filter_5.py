from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Filter_5(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((~ col("c_string").like(Config.c_regex1) & ~ col("c_string").like(Config.c_regex2)))
