from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from org.donot.openme12.config.ConfigStore import *
from org.donot.openme12.udfs.UDFs import *

def Reformat_5(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(col("c_short"), col("c_int"))