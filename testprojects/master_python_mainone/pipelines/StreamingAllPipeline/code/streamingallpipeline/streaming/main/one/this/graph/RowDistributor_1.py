from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from streamingallpipeline.streaming.main.one.this.config.ConfigStore import *
from streamingallpipeline.streaming.main.one.this.udfs.UDFs import *

def RowDistributor_1(spark: SparkSession, in0: DataFrame) -> (DataFrame, DataFrame):
    df1 = in0.filter((col("c_tinyint") > lit(-100)))
    df2 = in0.filter((col("c_int") > lit(-100)))

    return df1, df2
