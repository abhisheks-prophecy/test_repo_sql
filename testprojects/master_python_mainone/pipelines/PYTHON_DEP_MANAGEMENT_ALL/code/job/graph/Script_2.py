from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Script_2(spark: SparkSession, in0: DataFrame):
    print("hello how are you sir")
    out0 = in0
    in0.show()
    print("i am fine sir")

    return 
