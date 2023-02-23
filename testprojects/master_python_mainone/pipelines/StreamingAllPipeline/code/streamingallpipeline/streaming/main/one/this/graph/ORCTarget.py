from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from streamingallpipeline.streaming.main.one.this.config.ConfigStore import *
from streamingallpipeline.streaming.main.one.this.udfs.UDFs import *

def ORCTarget(spark: SparkSession, in0: DataFrame):
    in0.writeStream\
        .format("orc")\
        .option("path", "s3a://qa-prophecy/streaming/target/orc/all_type_with_partition")\
        .option("checkpointLocation", "s3a://qa-prophecy/streaming/target/orc/checkpoint_all_type_with_partition")\
        .queryName("ORCTarget_-8iObgDhU9KoAjbT2B9X8$$52oWzUcY-cOjBpsjQZYmP")\
        .partitionBy("p_int", "p_float", "p_string")\
        .start()
