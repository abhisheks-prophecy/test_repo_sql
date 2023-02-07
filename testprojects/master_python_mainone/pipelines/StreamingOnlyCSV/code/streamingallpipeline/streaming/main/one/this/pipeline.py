from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from streamingallpipeline.streaming.main.one.this.config.ConfigStore import *
from streamingallpipeline.streaming.main.one.this.udfs.UDFs import *
from prophecy.utils import *
from streamingallpipeline.streaming.main.one.this.graph import *

def pipeline(spark: SparkSession) -> None:
    df_CSVAutoloader = CSVAutoloader(spark)
    df_Join_1 = Join_1(spark, df_CSVAutoloader, df_CSVAutoloader)
    CSVTarget(spark, df_Join_1)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("fs.s3a.secret.key", "6oy7IXWucG7WcOSSM3fzlqAY1UafKYqFd7zlQi9s")
    spark.conf.set("fs.s3a.access.key", "AKIAR6ESAR2JAQNZNVMH")
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/StreamingOnlyCSV")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAR6ESAR2JAQNZNVMH")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/StreamingOnlyCSV")
    pipeline(spark)
    
    spark.streams.resetTerminated()
    spark.streams.awaitAnyTermination()
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
