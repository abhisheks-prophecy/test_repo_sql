from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from streamingtestpipeline_1.config.ConfigStore import *
from streamingtestpipeline_1.udfs.UDFs import *

def Reformat_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("c_tinyint"), 
        col("c_smallint"), 
        col("c_int"), 
        col("c_bigint"), 
        col("c_float"), 
        col("c_double"), 
        col("c_string"), 
        col("c_boolean"), 
        col("c_array"), 
        col("c_struct"), 
        col("p_int"), 
        col("p_float"), 
        col("p_string")
    )
