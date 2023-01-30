from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from python_streaming_main.config.ConfigStore import *
from python_streaming_main.udfs.UDFs import *

def Deduplicate_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.dropDuplicates(["p_string"])