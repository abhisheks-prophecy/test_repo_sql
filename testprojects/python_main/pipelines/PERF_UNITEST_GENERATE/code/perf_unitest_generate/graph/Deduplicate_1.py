from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from perf_unitest_generate.config.ConfigStore import *
from perf_unitest_generate.udfs.UDFs import *

def Deduplicate_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.dropDuplicates(["account_open_date"])
