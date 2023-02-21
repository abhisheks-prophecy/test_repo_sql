from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from streamingallpipeline.streaming.main.one.this.config.ConfigStore import *
from streamingallpipeline.streaming.main.one.this.udfs.UDFs import *

def FlattenSchema_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("c_array", explode_outer("c_array"))\
        .select(col("c_tinyint"), col("c_smallint"), col("c_int"), col("c_bigint"), col("c_float"), col("c_double"), col("c_string"), col("c_boolean"), col("c_array"), col("c_struct.city").alias("c_struct-city"), col("c_struct.state").alias("c_struct-state"), col("c_struct.pin").alias("c_struct-pin"), col("p_int"), col("p_float"), col("p_string"), col("c_new_col"))
