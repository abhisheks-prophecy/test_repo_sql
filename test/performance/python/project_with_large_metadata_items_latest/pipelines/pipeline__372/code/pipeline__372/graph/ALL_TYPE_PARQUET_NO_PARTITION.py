from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from pipeline__372.config.ConfigStore import *
from pipeline__372.udfs.UDFs import *

def ALL_TYPE_PARQUET_NO_PARTITION(spark: SparkSession) -> DataFrame:
    return spark.read.format("parquet").load("dbfs:/Prophecy/qa_data/parquet/all_type_no_partition")
