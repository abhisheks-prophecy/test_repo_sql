from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from streamingtestpipeline_1.config.ConfigStore import *
from streamingtestpipeline_1.udfs.UDFs import *

def SchemaTransform_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("c_bigint_string_concated", concat(col("c_bigint"), col("c_string")))\
        .withColumn("c_bigint_added", ((col("c_bigint") + col("c_smallint")) - col("c_tinyint")))\
        .drop("c_bigint")\
        .drop("c_string")\
        .withColumnRenamed("c_float", "c_float_new")
