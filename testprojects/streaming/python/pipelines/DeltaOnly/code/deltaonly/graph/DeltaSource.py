from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from deltaonly.config.ConfigStore import *
from deltaonly.udfs.UDFs import *

def DeltaSource(spark: SparkSession) -> DataFrame:
    return spark.readStream.format("delta").load("s3a://qa-prophecy/tmp/s3_streaming_delta/")
