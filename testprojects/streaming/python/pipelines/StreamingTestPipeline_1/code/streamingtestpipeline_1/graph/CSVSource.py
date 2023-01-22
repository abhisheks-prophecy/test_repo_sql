from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from streamingtestpipeline_1.config.ConfigStore import *
from streamingtestpipeline_1.udfs.UDFs import *

def CSVSource(spark: SparkSession) -> DataFrame:
    return spark.readStream\
        .format("csv")\
        .option("latestFirst", True)\
        .option("fileNameOnly", True)\
        .option("cleanSource", "off")\
        .option("sourceArchiveDir", "s3a://qa-prophecy/tmp/s3_streaming_csv_archived/")\
        .option("pathGlobFilter", "*.csv")\
        .option("header", True)\
        .option("sep", ",")\
        .option("maxFilesPerTrigger", "1")\
        .schema(
          StructType([
            StructField("c_tinyint", StringType(), True), StructField("c_smallint", StringType(), True), StructField("c_int", StringType(), True), StructField("c_bigint", StringType(), True), StructField("c_float", StringType(), True), StructField("c_double", StringType(), True), StructField("c_string", StringType(), True), StructField("c_boolean", StringType(), True), StructField("p_int", IntegerType(), True), StructField("p_float", DoubleType(), True), StructField("p_string", StringType(), True)
        ])
        )\
        .load("s3a://qa-prophecy/tmp/s3_streaming_csv/")
