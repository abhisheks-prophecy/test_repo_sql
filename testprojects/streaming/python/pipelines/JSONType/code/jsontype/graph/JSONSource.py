from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jsontype.config.ConfigStore import *
from jsontype.udfs.UDFs import *

def JSONSource(spark: SparkSession) -> DataFrame:
    return spark.readStream\
        .format("cloudFiles")\
        .option("cloudFiles.format", "json")\
        .option("cloudFiles.allowOverwrites", True)\
        .option("cloudFiles.includeExistingFiles", False)\
        .option("cloudFiles.validateOptions", True)\
        .option("cloudFiles.schemaEvolutionMode", "failOnNewColumns")\
        .option("mergeSchema", True)\
        .option("ignoreCorruptFiles", True)\
        .schema(
          StructType([
            StructField("c_array", ArrayType(StringType(), True), True), StructField("c_bigint", LongType(), True), StructField("c_boolean", BooleanType(), True), StructField("c_double", DoubleType(), True), StructField("c_float", DoubleType(), True), StructField("c_int", LongType(), True), StructField("c_smallint", LongType(), True), StructField("c_string", StringType(), True), StructField("c_struct", StructType([
              StructField("city", StringType(), True), StructField("pin", StringType(), True), StructField("state", StringType(), True)
            ]), True), StructField("c_tinyint", LongType(), True), StructField("p_int", IntegerType(), True), StructField("p_float", DoubleType(), True), StructField("p_string", StringType(), True)
        ])
        )\
        .load("s3a://qa-prophecy/tmp/s3_streaming_json/")
