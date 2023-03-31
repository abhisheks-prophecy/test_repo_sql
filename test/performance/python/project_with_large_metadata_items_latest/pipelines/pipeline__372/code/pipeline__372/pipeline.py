from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pipeline__372.config.ConfigStore import *
from pipeline__372.udfs.UDFs import *
from prophecy.utils import *
from pipeline__372.graph import *

def pipeline(spark: SparkSession) -> None:
    df_ALL_TYPE_PARQUET_NO_PARTITION = ALL_TYPE_PARQUET_NO_PARTITION(spark)
    df_Source_3 = Source_3(spark)
    df_Automated_Source_clone_python = Automated_Source_clone_python(spark)
    df_ALL_TYPE_CSV = ALL_TYPE_CSV(spark)
    df_Source_4 = Source_4(spark)
    df_Source_1 = Source_1(spark)
    df_Source_2 = Source_2(spark)
    df_Source_0 = Source_0(spark)

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
