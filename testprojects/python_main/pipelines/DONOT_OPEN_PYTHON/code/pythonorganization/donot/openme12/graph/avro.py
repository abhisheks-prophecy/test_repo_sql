from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pythonorganization.donot.openme12.config.ConfigStore import *
from pythonorganization.donot.openme12.udfs.UDFs import *

def avro(spark: SparkSession) -> DataFrame:
    return spark.read.format("avro").load("dbfs:/Prophecy/qa_data/avro/CustomersDatasetInput.avro")
