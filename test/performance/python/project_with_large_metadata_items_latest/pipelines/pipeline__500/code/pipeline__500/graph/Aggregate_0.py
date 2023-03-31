from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from pipeline__500.config.ConfigStore import *
from pipeline__500.udfs.UDFs import *

def Aggregate_0(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.agg()
