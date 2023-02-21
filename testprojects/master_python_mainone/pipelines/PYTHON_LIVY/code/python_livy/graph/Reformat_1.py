from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from python_livy.config.ConfigStore import *
from python_livy.udfs.UDFs import *

def Reformat_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0
