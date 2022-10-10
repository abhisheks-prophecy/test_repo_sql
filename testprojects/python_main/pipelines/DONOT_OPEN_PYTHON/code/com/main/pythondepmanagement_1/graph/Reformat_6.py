from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from com.main.pythondepmanagement_1.config.ConfigStore import *
from com.main.pythondepmanagement_1.udfs.UDFs import *

def Reformat_6(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(col("`c- short`").alias("c- short"), col("`c  - int`").alias("c  - int"))
