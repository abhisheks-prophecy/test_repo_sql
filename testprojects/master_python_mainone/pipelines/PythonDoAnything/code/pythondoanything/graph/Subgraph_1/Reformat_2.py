from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from pythondoanything.config.ConfigStore import *
from pythondoanything.udfs.UDFs import *

def Reformat_2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        concat(
            lit(Config.c_boolean), 
            lit(" :: "), 
            lit(Config.c_double), 
            lit(" :: "), 
            lit(Config.c_float), 
            lit(" :: "), 
            lit(Config.c_int), 
            lit(" :: "), 
            lit(Config.c_long), 
            lit(" :: "), 
            lit(Config.c_short), 
            lit(" :: "), 
            lit(Config.c_string1)
          )\
          .alias("c_config_values"), 
        col("`c   short  --`"), 
        col("`c-int-column type`"), 
        col("`-- c-long`"), 
        col("`c-decimal`"), 
        col("`c  float`"), 
        col("`c--boolean`"), 
        col("`c- - -double`"), 
        col("`c___-- string`"), 
        col("`c  date`"), 
        col("c_timestamp"), 
        col("c_new_col")
    )
