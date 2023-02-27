from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def OrderBy_2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.orderBy(
        col("`- c long`").asc(), 
        col("`c- short`").asc(), 
        col("`c -  boolean _  `").asc(), 
        col("c_double").asc()
    )
