from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from streamingpipelinemain.config.ConfigStore import *
from streamingpipelinemain.udfs.UDFs import *

def SchemaTransform_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn("c_new_col", concat(col("c_boolean"), col("c_float")))
