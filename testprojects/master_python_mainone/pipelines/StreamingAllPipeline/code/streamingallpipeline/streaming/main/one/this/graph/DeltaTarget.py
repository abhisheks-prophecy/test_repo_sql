from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from streamingallpipeline.streaming.main.one.this.config.ConfigStore import *
from streamingallpipeline.streaming.main.one.this.udfs.UDFs import *

def DeltaTarget(spark: SparkSession, in0: DataFrame):
    in0.writeStream\
        .format("delta")\
        .option("checkpointLocation", "s3://qa-prophecy/streaming/target/delta/checkpoint_all_type_with_partition/")\
        .queryName("DeltaTarget_iU97NQe46DLb5MOuk-ujd$$ajnYATA1U40X-6HOsS7_6")\
        .option("optimizeWrite", True)\
        .outputMode("append")\
        .partitionBy("p_int", "p_float", "p_string")\
        .start("s3a://qa-prophecy/streaming/target/delta/all_type_with_partition/")
