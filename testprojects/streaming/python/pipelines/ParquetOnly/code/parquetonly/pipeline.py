from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from parquetonly.config.ConfigStore import *
from parquetonly.udfs.UDFs import *
from prophecy.utils import *
from parquetonly.graph import *

def pipeline(spark: SparkSession) -> None:
    df_ParquetSrc = ParquetSrc(spark)
    df_Reformat_1 = Reformat_1(spark, df_ParquetSrc)
    ParquetTarget(spark, df_Reformat_1)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/ParquetOnly")
    
    MetricsCollector.start(
        spark = spark,
        pipelineId = spark.conf.get("prophecy.project.id") + "/" + "pipelines/ParquetOnly"
    )
    pipeline(spark)
    
    spark.streams.resetTerminated()
    spark.streams.awaitAnyTermination()
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
