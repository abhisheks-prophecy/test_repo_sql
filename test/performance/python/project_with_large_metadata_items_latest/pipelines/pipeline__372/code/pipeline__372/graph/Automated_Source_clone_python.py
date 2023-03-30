from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from pipeline__372.config.ConfigStore import *
from pipeline__372.udfs.UDFs import *

def Automated_Source_clone_python(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("jdbc")\
        .option("url", f"jdbc:mysql://{Config.JDBC_HOST}:3306/{Config.JDBC_DATABASE}")\
        .option("user", f"{Config.JDBC_USER}")\
        .option("password", f"admin")\
        .option("dbtable", Config.SOURCE_TABLE)\
        .option("pushDownPredicate", True)\
        .option("driver", "com.mysql.jdbc.Driver")\
        .load()
