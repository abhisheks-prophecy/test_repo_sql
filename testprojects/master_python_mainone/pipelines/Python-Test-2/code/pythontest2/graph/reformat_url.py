from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from pythontest2.config.ConfigStore import *
from pythontest2.udfs.UDFs import *

def reformat_url(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("method"), 
        col("coin"), 
        col("currency"), 
        concat(lit("https://rest.coinapi.io/v1/exchangerate/"), col("coin"), lit("/"), col("currency")).alias("url")
    )
