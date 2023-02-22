from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def FlattenSchema_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("c_array-- boolean ", explode_outer("c_array-- boolean "))\
        .withColumn("c_array-string  _ string", explode_outer("c_array-string  _ string"))\
        .withColumn("c_array-int  _ int", explode_outer("c_array-int  _ int"))\
        .withColumn("c_array--long", explode_outer("c_array--long"))\
        .select(col("c_array-int  _ int"), col("c_array-string  _ string"), col("c_array--long"), col("c_array-- boolean "), col("p_short"), col("p_int"), col("c_decimal  -  "))
