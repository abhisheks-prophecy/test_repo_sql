from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jsontype.config.ConfigStore import *
from jsontype.udfs.UDFs import *

def JSONTarget(spark: SparkSession, in0: DataFrame):
    in0.writeStream\
        .format("json")\
        .option("path", "s3a://qa-prophecy/tmp/dest_s3_streaming_json/")\
        .option("checkpointLocation", "s3a://qa-prophecy/tmp/dest_s3_checkpoint_streaming_json/")\
        .queryName("JSONTarget_qeiLdUT7xYUQ1chM0xmxg$$uBKOefU3QChYNFhWtVQvb")\
        .option("ignoreNullFields", True)\
        .partitionBy("p_string", "p_float", "p_int")\
        .start()
