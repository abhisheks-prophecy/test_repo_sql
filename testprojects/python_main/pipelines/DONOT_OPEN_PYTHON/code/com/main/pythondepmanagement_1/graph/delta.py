from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from com.main.pythondepmanagement_1.config.ConfigStore import *
from com.main.pythondepmanagement_1.udfs.UDFs import *

def delta(spark: SparkSession) -> DataFrame:
    return spark.read.format("delta").load("dbfs:/Prophecy/qa_data/delta/all_type_no_partition")
