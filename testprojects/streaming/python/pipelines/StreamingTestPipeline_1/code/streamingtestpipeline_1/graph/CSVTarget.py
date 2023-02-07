from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from streamingtestpipeline_1.config.ConfigStore import *
from streamingtestpipeline_1.udfs.UDFs import *

def CSVTarget(spark: SparkSession, in0: DataFrame):
    in0.writeStream\
        .format("csv")\
        .option("checkpointLocation", "s3a://qa-prophecy/tmp/dest_s3_checkpoint_streaming_csv/")\
        .queryName("StreamingTarget_1_LB6G0T5e_InINubMpB8qB$$SyDQA7EspRqmzcKICGHDJ")\
        .option("header", True)\
        .option("dateFormat", "yyyy-MM-dd")\
        .option("escape", "\\")\
        .option("emptyValue", "empty")\
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")\
        .option("quote", ",")\
        .option("sep", ",")\
        .option("quoteAll", True)\
        .option("encoding", "")\
        .option("charToEscapeQuoteEscaping", "\\\\")\
        .option("ignoreLeadingWhiteSpace", True)\
        .option("ignoreTrailingWhiteSpace", True)\
        .option("nullValue", "null")\
        .option("compression", "snappy")\
        .option("lineSep", "\\n")\
        .outputMode("append")\
        .partitionBy("p_int", "p_float", "p_string")\
        .option("path", "s3a://qa-prophecy/tmp/dest_s3_streaming_csv/")\
        .start()
