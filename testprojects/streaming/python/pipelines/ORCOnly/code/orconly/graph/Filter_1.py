from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from orconly.config.ConfigStore import *
from orconly.udfs.UDFs import *

def Filter_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(lit(True))
