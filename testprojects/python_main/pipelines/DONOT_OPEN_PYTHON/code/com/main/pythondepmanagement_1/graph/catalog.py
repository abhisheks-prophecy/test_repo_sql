from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from com.main.pythondepmanagement_1.config.ConfigStore import *
from com.main.pythondepmanagement_1.udfs.UDFs import *

def catalog(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"qa_database.test_catalog_source")
