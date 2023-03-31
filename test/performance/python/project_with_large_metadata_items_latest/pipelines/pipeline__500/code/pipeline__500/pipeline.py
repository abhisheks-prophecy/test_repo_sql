from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pipeline__500.config.ConfigStore import *
from pipeline__500.udfs.UDFs import *
from prophecy.utils import *
from pipeline__500.graph import *

def pipeline(spark: SparkSession) -> None:
    df_Aggregate_0 = Aggregate_0(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/pipeline__500")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/pipeline__500")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
