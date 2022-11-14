from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def SQLStatement_1(spark: SparkSession, in0: DataFrame, in1: DataFrame) -> (DataFrame, DataFrame):
    in0.createOrReplaceTempView("in0")
    in1.createOrReplaceTempView("in1")
    df1 = spark.sql(
        "select in0.p_int, in1.p_short, in0.`c_array-- boolean ` from in0, in1 where in0.p_int > 5 limit 10"
    )
    df2 = spark.sql(
        "select in0.p_int, in1.p_short, in0.`c_array-- boolean ` from in0, in1 where in1.p_int < 5 limit 20"
    )

    return df1, df2
