from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from streamingallpipeline.streaming.main.one.this.config.ConfigStore import *
from streamingallpipeline.streaming.main.one.this.udfs.UDFs import *

def StreamingTarget_1(spark: SparkSession, in0: DataFrame):
    from prophecy.utils import splunkHECForEachWriter # noqa
    in0.writeStream\
        .option("checkpointLocation", "/tmp/qa/randomtest1")\
        .queryName("StreamingTarget_1_Uhnv2F3s4OGhimwLy1iX8$$CXVUkehuDdQQFRbYcN7ck")\
        .foreachBatch(
          splunkHECForEachWriter(
            {
              'url': "http://ec2-3-22-233-65.us-east-2.compute.amazonaws.com:8000/",
              'token': "28d00425-3400-468b-ad4b-52a458b33e9a",
              'backoffFactor': "1",
              'maxRetries': "10",
              'maxPayload': None,
              'stopOnFailure': False
            }
          )
        )\
        .start()
