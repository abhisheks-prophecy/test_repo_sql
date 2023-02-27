from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def OrderBy_5(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.orderBy(col("c1").asc(), col("c2").asc(), col("c3").asc(), col("c4").asc(), col("c6").asc())
