from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jsontype.config.ConfigStore import *
from jsontype.udfs.UDFs import *

def WindowFunction_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("p_string", row_number().over(Window.partitionBy(col("p_int")).orderBy(col("p_float").asc())))\
        .withColumn("c_tinyint", row_number().over(Window.partitionBy(col("p_int")).orderBy(col("p_float").asc())))
