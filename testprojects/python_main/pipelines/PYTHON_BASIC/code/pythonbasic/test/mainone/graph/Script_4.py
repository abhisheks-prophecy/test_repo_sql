from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pythonbasic.test.mainone.config.ConfigStore import *
from pythonbasic.test.mainone.udfs.UDFs import *

def Script_4(spark: SparkSession, in0: DataFrame) -> DataFrame:
    print("hello")
    out0 = in0

    return out0
