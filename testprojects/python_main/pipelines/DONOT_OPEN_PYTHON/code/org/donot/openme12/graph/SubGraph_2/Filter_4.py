from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from org.donot.openme12.config.ConfigStore import *
from org.donot.openme12.udfs.UDFs import *

def Filter_4(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("`c- short`") > lit(-1)))
