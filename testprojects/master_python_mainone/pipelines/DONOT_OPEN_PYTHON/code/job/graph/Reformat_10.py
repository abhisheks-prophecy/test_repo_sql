from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Reformat_10(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(col("col1"), col("col2"), col("col3"))
