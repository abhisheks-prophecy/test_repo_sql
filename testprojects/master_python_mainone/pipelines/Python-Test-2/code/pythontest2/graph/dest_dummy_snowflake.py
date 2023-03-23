from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from pythontest2.config.ConfigStore import *
from pythontest2.udfs.UDFs import *

def dest_dummy_snowflake(spark: SparkSession, in0: DataFrame):
    from pyspark.dbutils import DBUtils
    options = {
        "sfUrl": "",
        "sfUser": "random",
        "sfPassword": "random",
        "sfDatabase": "",
        "sfSchema": "",
        "sfWarehouse": ""
    }
    writer = in0.write.format("snowflake").options(**options)
    writer.option("dbtable", "random").mode("overwrite").save()
