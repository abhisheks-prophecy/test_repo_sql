from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sqssource.config.ConfigStore import *
from sqssource.udfs.UDFs import *
from prophecy.utils import *
from sqssource.graph import *

def pipeline(spark: SparkSession) -> None:
    df_StreamingSource_0 = StreamingSource_0(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SQSSource")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SQSSource")
    pipeline(spark)
    
    spark.streams.resetTerminated()
    spark.streams.awaitAnyTermination()
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
