from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from com.main1.pythondepmanagement_1.config.ConfigStore import *
from com.main1.pythondepmanagement_1.udfs.UDFs import *

def SchemaTransform_1_1_1_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.drop("cmls_3ds_authntn_mthd")