from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Script_6(
        spark: SparkSession, 
        in0: DataFrame, 
        in1: DataFrame, 
        in2: DataFrame, 
        in3: DataFrame, 
        in4: DataFrame, 
        in5: DataFrame, 
        in6: DataFrame
) -> DataFrame:
    result = Config.c_0 * Config.c_1
    print(
        f"Configs are: CONFIG_STR:{Config.CONFIG_STR} \n CONFIG_DOUBLE:{Config.CONFIG_DOUBLE} \n CONFIG_BOOLEAN:{Config.CONFIG_BOOLEAN} \n CONFIG_INT:{Config.CONFIG_INT} \n CONFIG_FLOAT:{Config.CONFIG_FLOAT} \n CONFIG_SHORT:{Config.CONFIG_SHORT} \n CONFIG_DB_SECRETS:{Config.CONFIG_DB_SECRETS}"
    )
    assert(result == 0)
    out00 = in0.select("c-string").withColumnRenamed("c-string", "c_str_new")
    out1 = in1.select("c-string").withColumnRenamed("c-string", "c_str_new")
    out2 = in2.select("c-string").withColumnRenamed("c-string", "c_str_new")
    out3 = in3.select("c-string").withColumnRenamed("c-string", "c_str_new")
    out4 = in4.select("c-string").withColumnRenamed("c-string", "c_str_new")
    out5 = in5.select("c-string").withColumnRenamed("c-string", "c_str_new")
    out6 = in6.select("c-string").withColumnRenamed("c-string", "c_str_new")
    out0 = out00.union(out1).union(out2).union(out3).union(out4).union(out5).union(out6)

    return out0
