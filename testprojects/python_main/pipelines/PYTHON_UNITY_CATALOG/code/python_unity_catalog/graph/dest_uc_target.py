from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from python_unity_catalog.config.ConfigStore import *
from python_unity_catalog.udfs.UDFs import *

def dest_uc_target(spark: SparkSession, in0: DataFrame):
    in0.write.format("parquet").mode("overwrite").save("dbfs:/tmp/out_test1")
