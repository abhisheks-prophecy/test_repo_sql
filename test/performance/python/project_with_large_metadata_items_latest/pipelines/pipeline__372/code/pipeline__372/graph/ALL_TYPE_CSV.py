from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from pipeline__372.config.ConfigStore import *
from pipeline__372.udfs.UDFs import *

def ALL_TYPE_CSV(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("c_short", ShortType(), True), StructField("c_int", IntegerType(), True), StructField("c_long", LongType(), True), StructField("c_decimal", DecimalType(20, 10), True), StructField("c_float", FloatType(), True), StructField("c_boolean", BooleanType(), True), StructField("c_double", DoubleType(), True), StructField("c_string", StringType(), True), StructField("c_date", DateType(), True), StructField("c_timestamp", TimestampType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", ",")\
        .csv("dbfs:/Prophecy/qa_data/csv/all_type_no_partition")
