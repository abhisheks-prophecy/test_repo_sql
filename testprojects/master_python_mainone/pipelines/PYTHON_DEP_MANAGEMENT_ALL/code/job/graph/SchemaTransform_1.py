from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def SchemaTransform_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("concat short int", expr(Config.c_st_expr))\
        .drop("c- - -double")\
        .withColumnRenamed("c-decimal", Config.c_st_renamed)
