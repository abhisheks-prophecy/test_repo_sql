from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Script_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    out0 = in0

    return out0
