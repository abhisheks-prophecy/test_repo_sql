from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from org.donot.openme12.config.ConfigStore import *
from org.donot.openme12.udfs.UDFs import *

def OrderBy_4(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.orderBy(col("c_short").asc())
