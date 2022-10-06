from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from a_usingwalipipeline.config.ConfigStore import *
from a_usingwalipipeline.udfs.UDFs import *

def TEST_SHARINGWALADATASET(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("customer_id", StringType(), True), StructField("first_name", StringType(), True), StructField("last_name", StringType(), True), StructField("phone", StringType(), True), StructField("email", StringType(), True), StructField("country_code", StringType(), True), StructField("account_open_date", StringType(), True), StructField("account_flags", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", ",")\
        .csv("dbfs:/Prophecy/qa_data/csv/CustomersDatasetInputWithHeader.csv")
