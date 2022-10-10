from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pythongenericuse.config.ConfigStore import *
from pythongenericuse.udfs.UDFs import *

def Source_1_1_1(spark: SparkSession) -> DataFrame:
    return spark.read.format("parquet").load("dbfs:/Prophecy/qa_data/parquet/all_type_and_partition_withspacehyphens")
