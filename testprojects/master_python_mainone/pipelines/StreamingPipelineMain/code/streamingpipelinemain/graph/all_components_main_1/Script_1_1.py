from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from streamingpipelinemain.config.ConfigStore import *
from streamingpipelinemain.udfs.UDFs import *

def Script_1_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    out0 = in0

    return out0
