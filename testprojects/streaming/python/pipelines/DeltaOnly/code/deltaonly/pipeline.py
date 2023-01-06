from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from deltaonly.config.ConfigStore import *
from deltaonly.udfs.UDFs import *
from prophecy.utils import *
from deltaonly.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DeltaSource = DeltaSource(spark)
    df_Limit_2 = Limit_2(spark, df_DeltaSource)
    DeltaTarget(spark, df_Limit_2)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/DeltaOnly")
    
    MetricsCollector.start(spark = spark, pipelineId = spark.conf.get("prophecy.project.id") + "/" + "pipelines/DeltaOnly")
    pipeline(spark)
    
    spark.streams.resetTerminated()
    spark.streams.awaitAnyTermination()
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
