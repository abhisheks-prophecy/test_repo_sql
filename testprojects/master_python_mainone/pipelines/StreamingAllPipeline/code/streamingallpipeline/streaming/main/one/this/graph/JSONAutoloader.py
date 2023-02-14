from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from streamingallpipeline.streaming.main.one.this.config.ConfigStore import *
from streamingallpipeline.streaming.main.one.this.udfs.UDFs import *

def JSONAutoloader(spark: SparkSession) -> DataFrame:
    spark.conf.set("cloudFiles.schemaInference.sampleSize.numBytes", "10mb")
    spark.conf.set("cloudFiles.schemaInference.sampleSize.numFiles", "1")

    return spark.readStream\
        .format("cloudFiles")\
        .option("cloudFiles.format", "json")\
        .option("cloudFiles.useIncrementalListing", "auto")\
        .option("cloudFiles.allowOverwrites", False)\
        .option("cloudFiles.includeExistingFiles", True)\
        .option("cloudFiles.validateOptions", True)\
        .option("cloudFiles.inferColumnTypes", True)\
        .option("cloudFiles.schemaEvolutionMode", "failOnNewColumns")\
        .option("pathGlobFilter", "*.json")\
        .option("mergeSchema", False)\
        .option("recursiveFileLookup", False)\
        .option("ignoreCorruptFiles", False)\
        .schema(
          StructType([
            StructField("c_array", ArrayType(StringType(), True), True), StructField("c_bigint", LongType(), True), StructField("c_boolean", BooleanType(), True), StructField("c_double", DoubleType(), True), StructField("c_float", DoubleType(), True), StructField("c_int", LongType(), True), StructField("c_smallint", LongType(), True), StructField("c_string", StringType(), True), StructField("c_struct", StructType([
              StructField("city", StringType(), True), StructField("pin", StringType(), True), StructField("state", StringType(), True)
            ]), True), StructField("c_tinyint", LongType(), True), StructField("p_int", IntegerType(), True), StructField("p_float", DoubleType(), True), StructField("p_string", StringType(), True)
        ])
        )\
        .load("s3a://qa-prophecy/streaming/source/json/all_type_with_partition")
