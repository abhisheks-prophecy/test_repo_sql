from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from streamingtestpipeline_1.config.ConfigStore import *
from streamingtestpipeline_1.udfs.UDFs import *

def DeltaSource_1(spark: SparkSession) -> DataFrame:
    return spark.readStream.format("delta").load("s3a://qa-prophecy/tmp/s3_streaming_delta/")
