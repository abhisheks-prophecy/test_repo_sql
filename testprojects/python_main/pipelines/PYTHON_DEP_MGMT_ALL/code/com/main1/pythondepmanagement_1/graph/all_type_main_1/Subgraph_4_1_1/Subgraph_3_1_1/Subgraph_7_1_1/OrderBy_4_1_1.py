from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from com.main1.pythondepmanagement_1.config.ConfigStore import *
from com.main1.pythondepmanagement_1.udfs.UDFs import *

def OrderBy_4_1_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.orderBy(col("`c-string`").asc())
