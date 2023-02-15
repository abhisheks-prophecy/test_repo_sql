from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from streamingpipelinemain.config.ConfigStore import *
from streamingpipelinemain.udfs.UDFs import *

def ParquetTarget2(spark: SparkSession, in0: DataFrame):
    in0.writeStream\
        .format("parquet")\
        .option("path", "s3a://qa-prophecy/streaming/target/parquet/1all_type_with_partition")\
        .option("checkpointLocation", "s3a://qa-prophecy/streaming/target/parquet/checkpoint_1all_type_with_partition")\
        .queryName("ParquetTarget2_7m0dgvVNoI7rrhCk_TfX7$$MEq-GqIJd1iOvYd9XbY8y")\
        .outputMode("append")\
        .start()