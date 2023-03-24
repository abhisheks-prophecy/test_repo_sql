from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from streamingpipelinemain.config.ConfigStore import *
from streamingpipelinemain.udfs.UDFs import *

def Reformat_8_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0
