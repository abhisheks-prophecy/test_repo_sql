from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pythonorganization.donot.openme12.config.ConfigStore import *
from pythonorganization.donot.openme12.udfs.UDFs import *

def OrderBy_3(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.orderBy(
        col("customer_id").asc(), 
        col("first_name").asc(), 
        col("last_name").asc(), 
        col("phone").asc(), 
        col("email").asc(), 
        col("country_code").asc(), 
        col("account_open_date").asc(), 
        col("account_flags").asc()
    )
