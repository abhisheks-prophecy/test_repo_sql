from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from parquetonly.config.ConfigStore import *
from parquetonly.udfs.UDFs import *

def ParquetSrc(spark: SparkSession) -> DataFrame:
    return spark.readStream.format("parquet").load("s3a://qa-prophecy/tmp/s3_streaming_parquet/")
