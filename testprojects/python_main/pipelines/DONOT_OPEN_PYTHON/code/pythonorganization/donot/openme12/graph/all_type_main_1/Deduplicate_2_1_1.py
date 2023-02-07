from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pythonorganization.donot.openme12.config.ConfigStore import *
from pythonorganization.donot.openme12.udfs.UDFs import *

def Deduplicate_2_1_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.dropDuplicates(["c-string"])
