from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pipe1.config.ConfigStore import *
from pipe1.udfs.UDFs import *

def d1(spark: SparkSession) -> DataFrame:
    return spark.read.option("header", True).option("sep", ",").csv("test")
