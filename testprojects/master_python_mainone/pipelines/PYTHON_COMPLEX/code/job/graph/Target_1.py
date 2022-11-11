from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Target_1(spark: SparkSession, in0: DataFrame):
    in0.write.format("json").mode("overwrite").save("dbfs:/tmp/e2e/dataset_out_76008")
