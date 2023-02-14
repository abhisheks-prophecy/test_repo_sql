from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from streamingallpipeline.streaming.main.one.this.config.ConfigStore import *
from streamingallpipeline.streaming.main.one.this.udfs.UDFs import *

def DELTASource(spark: SparkSession) -> DataFrame:
    return spark.readStream.format("delta").load("s3a://qa-prophecy/streaming/source/delta/all_type_with_partition")