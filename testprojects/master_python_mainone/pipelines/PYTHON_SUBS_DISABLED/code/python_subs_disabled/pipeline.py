from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from python_subs_disabled.config.ConfigStore import *
from python_subs_disabled.udfs.UDFs import *
from prophecy.utils import *
from python_subs_disabled.graph import *

def pipeline(spark: SparkSession) -> None:
    df_src_delta_all_type_no_partition = src_delta_all_type_no_partition(spark)
    df_Reformat_1 = Reformat_1(spark, df_src_delta_all_type_no_partition)
    dest_delta_scd2(spark, df_Reformat_1)
    Target_1(spark, df_Reformat_1)
    df_Source_1 = Source_1(spark)
    df_RestAPIEnrich_1 = RestAPIEnrich_1(spark, df_Reformat_1)
    dest_delta_merge(spark, df_Reformat_1)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/PYTHON_SUBS_DISABLED")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/PYTHON_SUBS_DISABLED")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
