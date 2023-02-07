from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from streamingallpipeline.streaming.main.one.this.config.ConfigStore import *
from streamingallpipeline.streaming.main.one.this.udfs.UDFs import *

def CSVAutoloader(spark: SparkSession) -> DataFrame:
    return spark.readStream\
        .format("cloudFiles")\
        .option("cloudFiles.format", "csv")\
        .option("cloudFiles.allowOverwrites", True)\
        .option("cloudFiles.includeExistingFiles", True)\
        .option("modifiedBefore", "2090-01-01 05:05:05")\
        .option("modifiedAfter", "2018-10-10 10:45:55")\
        .option("ignoreCorruptFiles", True)\
        .option("header", True)\
        .option("sep", ",")\
        .option("cloudFiles.maxFilesPerTrigger", "1")\
        .schema(
          StructType([
            StructField("c_tinyint", StringType(), True), StructField("c_smallint", StringType(), True), StructField("c_int", StringType(), True), StructField("c_bigint", StringType(), True), StructField("c_float", StringType(), True), StructField("c_double", StringType(), True), StructField("c_string", StringType(), True), StructField("c_boolean", StringType(), True), StructField("p_int", IntegerType(), True), StructField("p_float", DoubleType(), True), StructField("p_string", StringType(), True)
        ])
        )\
        .load("s3a://qa-prophecy/streaming/source/csv/all_type_with_partition")
