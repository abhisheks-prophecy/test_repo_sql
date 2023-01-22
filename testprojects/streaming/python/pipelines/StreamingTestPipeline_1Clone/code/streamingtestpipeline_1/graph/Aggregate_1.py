from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from streamingtestpipeline_1.config.ConfigStore import *
from streamingtestpipeline_1.udfs.UDFs import *

def Aggregate_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("p_int"), col("p_float"), col("p_string"))

    return df1.agg(
        first(col("c_tinyint")).alias("c_tinyint"), 
        first(col("c_boolean")).alias("c_boolean"), 
        first(col("c_double")).alias("c_double")
    )
