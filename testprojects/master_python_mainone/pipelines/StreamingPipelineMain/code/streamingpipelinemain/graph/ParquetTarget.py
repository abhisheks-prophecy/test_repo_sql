from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from streamingpipelinemain.config.ConfigStore import *
from streamingpipelinemain.udfs.UDFs import *

def ParquetTarget(spark: SparkSession, in0: DataFrame):
    in0.writeStream\
        .format("parquet")\
        .option("path", "s3a://qa-prophecy/streaming/target/parquet/all_type_with_partition")\
        .option("checkpointLocation", "s3a://qa-prophecy/streaming/target/parquet/checkpoint_all_type_with_partition")\
        .queryName("ParquetTarget_ZB6A9IbyBLQWt2vodLGCh$$nndaM4nU87ORZU79K3EDC")\
        .option("compression", "gzip")\
        .outputMode("append")\
        .partitionBy("p_int", "p_float", "p_string")\
        .start()
