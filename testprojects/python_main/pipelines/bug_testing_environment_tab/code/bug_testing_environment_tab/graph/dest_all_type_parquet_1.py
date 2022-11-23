from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from bug_testing_environment_tab.config.ConfigStore import *
from bug_testing_environment_tab.udfs.UDFs import *

def dest_all_type_parquet_1(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"qa_database.dest_all_type_parquet")
