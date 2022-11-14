from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Deduplicate_2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.dropDuplicates(["c_timestamp", "c_decimal"])
