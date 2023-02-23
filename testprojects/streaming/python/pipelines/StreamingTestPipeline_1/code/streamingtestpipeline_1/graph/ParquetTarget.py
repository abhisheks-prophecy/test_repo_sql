from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from streamingtestpipeline_1.config.ConfigStore import *
from streamingtestpipeline_1.udfs.UDFs import *

def ParquetTarget(spark: SparkSession, in0: DataFrame):
    in0.writeStream\
        .format("parquet")\
        .option("path", "s3a://qa-prophecy/tmp/dest_s3_streaming_parquet/")\
        .option("checkpointLocation", "s3a://qa-prophecy/tmp/checkpoint_dest_s3_streaming_parquet/")\
        .queryName("StreamingTarget_1_X8yGA7QSmYG2XqLwOeDnA$$niUHIB46EIWvT63N2bpWT")\
        .option("compression", "gzip")\
        .outputMode("update")\
        .partitionBy("p_int", "p_float", "p_string")\
        .start()
