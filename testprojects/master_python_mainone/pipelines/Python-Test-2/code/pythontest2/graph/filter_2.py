from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from pythontest2.config.ConfigStore import *
from pythontest2.udfs.UDFs import *

def filter_2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(lit(True))
