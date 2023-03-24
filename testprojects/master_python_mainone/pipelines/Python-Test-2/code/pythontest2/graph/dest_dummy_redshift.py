from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from pythontest2.config.ConfigStore import *
from pythontest2.udfs.UDFs import *

def dest_dummy_redshift(spark: SparkSession, in0: DataFrame):
    from pyspark.dbutils import DBUtils
    in0.write\
        .format("com.databricks.spark.redshift")\
        .option("url", "random")\
        .option("user", DBUtils(spark).secrets.get(scope = "random", key = f"random"))\
        .option("password", DBUtils(spark).secrets.get(scope = "random", key = f"random"))\
        .option("url", "random")\
        .save()
