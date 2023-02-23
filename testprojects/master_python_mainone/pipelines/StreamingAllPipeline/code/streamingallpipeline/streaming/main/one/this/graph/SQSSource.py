from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from streamingallpipeline.streaming.main.one.this.config.ConfigStore import *
from streamingallpipeline.streaming.main.one.this.udfs.UDFs import *

def SQSSource(spark: SparkSession) -> DataFrame:
    return spark.readStream\
        .format("cloudFiles")\
        .option("cloudFiles.format", "parquet")\
        .option("cloudFiles.useNotifications", True)\
        .option("cloudFiles.queueUrl", "https://sqs.us-west-1.amazonaws.com/133450206866/qa-test-streaming")\
        .schema(
          StructType([
            StructField("c_tinyint", ByteType(), True), StructField("c_smallint", ShortType(), True), StructField("c_int", IntegerType(), True), StructField("c_bigint", LongType(), True), StructField("c_float", FloatType(), True), StructField("c_double", DoubleType(), True), StructField("c_string", StringType(), True), StructField("c_boolean", BooleanType(), True), StructField("c_array", ArrayType(StringType(), True), True), StructField("c_struct", StructType([
              StructField("city", StringType(), True), StructField("state", StringType(), True), StructField("pin", LongType(), True)
            ]), True), StructField("p_int", IntegerType(), True), StructField("p_float", DoubleType(), True), StructField("p_string", StringType(), True)
        ])
        )\
        .load()
