from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from streamingtestpipeline_1.config.ConfigStore import *
from streamingtestpipeline_1.udfs.UDFs import *

def Limit_2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.limit(10)
