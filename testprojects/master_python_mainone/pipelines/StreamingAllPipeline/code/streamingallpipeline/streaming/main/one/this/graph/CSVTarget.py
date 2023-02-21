from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from streamingallpipeline.streaming.main.one.this.config.ConfigStore import *
from streamingallpipeline.streaming.main.one.this.udfs.UDFs import *

def CSVTarget(spark: SparkSession, in0: DataFrame):
    in0.writeStream\
        .format("csv")\
        .option("checkpointLocation", "s3a://qa-prophecy/streaming/target/csv/checkpoint_all_type_with_partition")\
        .queryName("CSVTarget_Cszid21DdK82pDYJJcZXp$$Bhc4Hgmwj8HEtLJE441xL")\
        .option("header", True)\
        .option("dateFormat", "yyyy-MM-dd")\
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")\
        .option("sep", ",")\
        .option("quoteAll", False)\
        .option("escapeQuotes", False)\
        .option("ignoreLeadingWhiteSpace", True)\
        .option("ignoreTrailingWhiteSpace", True)\
        .outputMode("append")\
        .partitionBy("p_int", "p_float", "p_string")\
        .trigger(processingTime = "1 minute")\
        .option("path", "s3a://qa-prophecy/streaming/target/csv/all_type_with_partition")\
        .start()
