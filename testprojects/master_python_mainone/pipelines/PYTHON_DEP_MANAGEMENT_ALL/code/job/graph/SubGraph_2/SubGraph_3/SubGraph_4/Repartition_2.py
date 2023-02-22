from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Repartition_2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.repartitionByRange(col("`c- short`").asc(), col("`c_float-__  `").desc())
