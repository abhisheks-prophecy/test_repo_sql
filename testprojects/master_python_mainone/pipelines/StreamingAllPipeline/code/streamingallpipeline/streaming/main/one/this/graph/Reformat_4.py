from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from streamingallpipeline.streaming.main.one.this.config.ConfigStore import *
from streamingallpipeline.streaming.main.one.this.udfs.UDFs import *

def Reformat_4(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0
