from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def custom_xlsx_py(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("excel")\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .load("dbfs:/FileStore/Users/abhinav/test_1.xlsx")
