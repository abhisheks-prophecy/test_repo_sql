from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from orconly.config.ConfigStore import *
from orconly.udfs.UDFs import *

def ORCTarget(spark: SparkSession, in0: DataFrame):
    in0.writeStream\
        .format("orc")\
        .option("path", "s3a://qa-prophecy/tmp/dest_s3_streaming_orc/")\
        .option("checkpointLocation", "s3a://qa-prophecy/tmp/dest_s3_checkpoint_streaming_orc/")\
        .queryName("ORCTarget_Etu-7YyjyRcfZuxflj7h0$$RQpx_LfqjIDBMh0DAQfF1")\
        .start()
