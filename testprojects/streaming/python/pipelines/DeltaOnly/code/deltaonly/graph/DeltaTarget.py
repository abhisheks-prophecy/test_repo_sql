from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from deltaonly.config.ConfigStore import *
from deltaonly.udfs.UDFs import *

def DeltaTarget(spark: SparkSession, in0: DataFrame):
    in0.writeStream\
        .format("delta")\
        .option("checkpointLocation", "s3a://qa-prophecy/tmp/dest_s3_checkpoint_streaming_delta/")\
        .queryName("StreamingTarget_1_V5VSLOOXBrErbskSbhola$$jFNNjb5-K5qCnTlvDStq2")\
        .outputMode("append")\
        .start("s3a://qa-prophecy/tmp/dest_s3_streaming_delta/")
