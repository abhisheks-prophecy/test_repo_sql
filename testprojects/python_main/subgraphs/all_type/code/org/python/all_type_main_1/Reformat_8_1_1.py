from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pythongenericuse.config.ConfigStore import *
from pythongenericuse.udfs.UDFs import *

def Reformat_8_1_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0
