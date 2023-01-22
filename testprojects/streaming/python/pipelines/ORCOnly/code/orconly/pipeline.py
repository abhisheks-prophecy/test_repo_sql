from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from orconly.config.ConfigStore import *
from orconly.udfs.UDFs import *
from prophecy.utils import *
from orconly.graph import *

def pipeline(spark: SparkSession) -> None:
    df_ORCSource = ORCSource(spark)
    df_Filter_1 = Filter_1(spark, df_ORCSource)
    ORCTarget(spark, df_Filter_1)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/ORCOnly")
    
    MetricsCollector.start(spark = spark, pipelineId = spark.conf.get("prophecy.project.id") + "/" + "pipelines/ORCOnly")
    pipeline(spark)
    
    spark.streams.resetTerminated()
    spark.streams.awaitAnyTermination()
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
