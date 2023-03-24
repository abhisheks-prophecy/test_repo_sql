from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from pythontest2.config.ConfigStore import *
from pythontest2.udfs.UDFs import *

def create_data(spark: SparkSession) -> DataFrame:
    
    out0 = spark\
               .createDataFrame([
('GET', 'DOGE', 'USD'), ('GET', 'DOGE', 'EUR'), ('GET', 'BTC', 'USD'), ('GET', 'BTC', 'EUR'), ])\
               .toDF('method', 'coin', 'currency')

    return out0
