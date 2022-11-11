from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def orc_src(spark: SparkSession) -> DataFrame:
    return spark.read.format("orc").load("dbfs:/Prophecy/qa_data/orc/all_type_no_partition")
