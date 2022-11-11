from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pythondoanything.config.ConfigStore import *
from pythondoanything.udfs.UDFs import *

def Filter_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("`c-string`").like("%3%") | col("`c-string`").like(Config.c_regex_filter)))
