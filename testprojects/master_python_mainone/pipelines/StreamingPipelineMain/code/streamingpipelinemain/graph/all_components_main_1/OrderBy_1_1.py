from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from streamingpipelinemain.config.ConfigStore import *
from streamingpipelinemain.udfs.UDFs import *

def OrderBy_1_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.orderBy(
        col("`c- short`").asc(), 
        col("`c  - int`").asc(), 
        col("`- c long`").asc(), 
        col("`c_decimal  -  `").asc(), 
        col("`c_float-__  `").asc()
    )
