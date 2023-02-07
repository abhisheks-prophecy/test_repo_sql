from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from csvonly.config.ConfigStore import *
from csvonly.udfs.UDFs import *
from prophecy.utils import *
from csvonly.graph import *

def pipeline(spark: SparkSession) -> None:
    df_CSVSource = CSVSource(spark)
    df_Subgraph_1 = Subgraph_1(spark, df_CSVSource)
    df_streamingsgLimit_1 = streamingsgLimit_1(spark, df_Subgraph_1)
    CSVTarget(spark, df_streamingsgLimit_1)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/CSVOnly")
    
    MetricsCollector.start(spark = spark, pipelineId = spark.conf.get("prophecy.project.id") + "/" + "pipelines/CSVOnly")
    pipeline(spark)
    
    spark.streams.resetTerminated()
    spark.streams.awaitAnyTermination()
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
