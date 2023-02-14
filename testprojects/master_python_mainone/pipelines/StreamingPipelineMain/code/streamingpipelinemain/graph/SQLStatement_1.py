from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from streamingpipelinemain.config.ConfigStore import *
from streamingpipelinemain.udfs.UDFs import *

def SQLStatement_1(spark: SparkSession, in0: DataFrame) -> (DataFrame):
    in0.createOrReplaceTempView("in0")
    df1 = spark.sql("select * from in0 where c_tinyint > -1000")

    return df1
