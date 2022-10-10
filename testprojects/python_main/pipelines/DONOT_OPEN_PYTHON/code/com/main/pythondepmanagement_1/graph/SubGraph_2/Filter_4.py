from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from com.main.pythondepmanagement_1.config.ConfigStore import *
from com.main.pythondepmanagement_1.udfs.UDFs import *

def Filter_4(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("`c- short`") > lit(-1)))
