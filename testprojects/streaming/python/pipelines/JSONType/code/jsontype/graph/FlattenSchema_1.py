from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jsontype.config.ConfigStore import *
from jsontype.udfs.UDFs import *

def FlattenSchema_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("c_array", explode_outer("c_array"))\
        .select(col("c_array"), col("c_struct.city").alias("c_struct-city"), col("c_struct.pin").alias("c_struct-pin"), col("c_struct.state").alias("c_struct-state"), col("c_tinyint"), col("p_int"), col("p_float"), col("p_string"))
