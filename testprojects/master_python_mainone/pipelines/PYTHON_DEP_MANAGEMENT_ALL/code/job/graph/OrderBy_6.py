from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def OrderBy_6(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.orderBy(col("p_int").asc(), col("`c_array-- boolean `").desc())
