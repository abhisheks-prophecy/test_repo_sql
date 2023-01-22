from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from orconly.config.ConfigStore import *
from orconly.udfs.UDFs import *

def ORCSource(spark: SparkSession) -> DataFrame:
    return spark.readStream\
        .format("cloudFiles")\
        .option("cloudFiles.format", "orc")\
        .load("s3a://qa-prophecy/tmp/s3_streaming_orc")
