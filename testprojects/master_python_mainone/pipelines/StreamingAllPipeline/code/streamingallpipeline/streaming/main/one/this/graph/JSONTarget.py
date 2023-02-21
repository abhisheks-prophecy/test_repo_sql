from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from streamingallpipeline.streaming.main.one.this.config.ConfigStore import *
from streamingallpipeline.streaming.main.one.this.udfs.UDFs import *

def JSONTarget(spark: SparkSession, in0: DataFrame):
    in0.writeStream\
        .format("json")\
        .option("path", "s3a://qa-prophecy/streaming/target/json/all_type_with_partition")\
        .option("checkpointLocation", "s3a://qa-prophecy/streaming/target/json/checkpoint_all_type_with_partition")\
        .queryName("JSONTarget_RcRayw_BtDoIwm3b68yfj$$qSRP9VPdht2Erdx64mM5l")\
        .outputMode("append")\
        .partitionBy("p_int", "p_float", "p_string")\
        .start()
