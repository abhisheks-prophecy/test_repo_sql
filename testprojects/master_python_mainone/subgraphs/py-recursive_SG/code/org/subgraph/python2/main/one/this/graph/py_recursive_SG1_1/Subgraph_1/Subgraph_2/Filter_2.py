from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from streamingallpipeline.streaming.main.one.this.config.ConfigStore import *
from streamingallpipeline.streaming.main.one.this.udfs.UDFs import *

def Filter_2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("`c  - int`") > lit(10)))
