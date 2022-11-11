from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Target_2(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("hive")\
        .option("fileFormat", "parquet")\
        .mode("overwrite")\
        .saveAsTable("qa_database.test_catalog_destination_1")
