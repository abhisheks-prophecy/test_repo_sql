from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from python_uc.config.ConfigStore import *
from python_uc.udfs.UDFs import *

def all_type_parquet(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"qa_database.all_type_parquet")
