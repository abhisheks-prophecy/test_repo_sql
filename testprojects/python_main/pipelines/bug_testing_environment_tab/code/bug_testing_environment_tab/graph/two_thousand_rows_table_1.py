from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from bug_testing_environment_tab.config.ConfigStore import *
from bug_testing_environment_tab.udfs.UDFs import *

def two_thousand_rows_table_1(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"qa_database.two_thousand_rows_table")
