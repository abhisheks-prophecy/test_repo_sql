from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from com.main1.pythondepmanagement_1.config.ConfigStore import *
from com.main1.pythondepmanagement_1.udfs.UDFs import *

def PERF_SCHEMATRANSFORM(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.drop("cmls_acqr_wrkstn_id")
