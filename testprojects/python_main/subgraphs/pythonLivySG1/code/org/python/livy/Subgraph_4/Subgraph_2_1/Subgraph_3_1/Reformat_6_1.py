from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from livy_python.config.ConfigStore import *
from livy_python.udfs.UDFs import *

def Reformat_6_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0
