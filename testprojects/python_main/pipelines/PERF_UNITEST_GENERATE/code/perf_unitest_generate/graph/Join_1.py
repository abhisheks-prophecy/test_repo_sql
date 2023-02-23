from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from perf_unitest_generate.config.ConfigStore import *
from perf_unitest_generate.udfs.UDFs import *

def Join_1(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.email") == col("in1.email")), "inner")\
        .select(col("in0.account_flags").alias("account_flags"), col("in0.account_open_date").alias("account_open_date"), col("in0.country_code").alias("country_code"), col("in0.customer_id").alias("customer_id"), col("in0.email").alias("email"), col("in0.first_name").alias("first_name"), col("in0.last_name").alias("last_name"), col("in0.phone").alias("phone"))
