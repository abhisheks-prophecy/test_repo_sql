from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from streamingtestpipeline_1.config.ConfigStore import *
from streamingtestpipeline_1.udfs.UDFs import *

def ORCSource(spark: SparkSession) -> DataFrame:
    return spark.readStream\
        .format("cloudFiles")\
        .option("cloudFiles.format", "orc")\
        .load("s3a://qa-prophecy/tmp/s3_streaming_orc")
