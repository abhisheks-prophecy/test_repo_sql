from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from pipeline__102.config.ConfigStore import *
from pipeline__102.udfs.UDFs import *

def a_table_with_data_1(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"qa_20k_tables_test.a_table_with_data_1")
