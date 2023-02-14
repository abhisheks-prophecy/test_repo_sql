from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from streamingallpipeline.streaming.main.one.this.config.ConfigStore import *
from streamingallpipeline.streaming.main.one.this.udfs.UDFs import *

def Join_1(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.c_tinyint") == col("in1.c_tinyint")), "inner")\
        .select(col("in0.c_tinyint").alias("c_tinyint"), col("in0.c_smallint").alias("c_smallint"), col("in0.c_int").alias("c_int"), col("in0.c_bigint").alias("c_bigint"), col("in0.c_float").alias("c_float"), col("in0.c_double").alias("c_double"), col("in0.c_string").alias("c_string"), col("in0.c_boolean").alias("c_boolean"), col("in0.p_int").alias("p_int"), col("in0.p_float").alias("p_float"), col("in0.p_string").alias("p_string"))