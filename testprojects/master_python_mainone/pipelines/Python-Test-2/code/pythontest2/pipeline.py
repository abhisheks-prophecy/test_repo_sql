from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pythontest2.config.ConfigStore import *
from pythontest2.udfs.UDFs import *
from prophecy.utils import *
from pythontest2.graph import *

def pipeline(spark: SparkSession) -> None:
    df_create_data = create_data(spark)
    df_reformat_url = reformat_url(spark, df_create_data)
    df_rate_from_api = rate_from_api(spark, df_reformat_url)
    df_src_delta_all_type_no_partition = src_delta_all_type_no_partition(spark)
    dest_dummy_delta(spark, df_src_delta_all_type_no_partition)
    df_parse_data = parse_data(spark, df_rate_from_api)
    df_create_data_1 = create_data_1(spark)
    df_data_from_api = data_from_api(spark, df_create_data_1)
    dest_dummy_delta_catalog_merge(spark, df_src_delta_all_type_no_partition)
    df_parse = parse(spark, df_data_from_api)
    dest_dummy_snowflake(spark, df_src_delta_all_type_no_partition)
    dest_dummy_delta_catalog_merge_scd(spark, df_src_delta_all_type_no_partition)
    dest_dummy_delta_scd2(spark, df_src_delta_all_type_no_partition)
    dest_dummy_redshift(spark, df_src_delta_all_type_no_partition)
    df_filter_1 = filter_1(spark, df_parse)
    df_filter_2 = filter_2(spark, df_parse_data)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/Python-Test-2")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/Python-Test-2")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
