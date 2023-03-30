from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pipeline__372.config.ConfigStore import *
from pipeline__372.udfs.UDFs import *
from prophecy.utils import *
from pipeline__372.graph import *

def pipeline(spark: SparkSession) -> None:
    pass

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/pipeline__372")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/pipeline__372")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
