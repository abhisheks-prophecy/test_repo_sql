from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from bug_testing_environment_tab.config.ConfigStore import *
from bug_testing_environment_tab.udfs.UDFs import *
from prophecy.utils import *
from bug_testing_environment_tab.graph import *

def pipeline(spark: SparkSession) -> None:
    df_dest_all_type_parquet_9 = dest_all_type_parquet_9(spark)
    df_dest_all_type_parquet_10 = dest_all_type_parquet_10(spark)
    df_dest_all_type_parquet_6 = dest_all_type_parquet_6(spark)
    df_all_type_parquet_0 = all_type_parquet_0(spark)
    df_dest_all_type_parquet_1 = dest_all_type_parquet_1(spark)
    df_all_type_parquet_1 = all_type_parquet_1(spark)
    df_all_type_parquet_2 = all_type_parquet_2(spark)
    df_dest_all_type_parquet_7 = dest_all_type_parquet_7(spark)
    df_dest_all_type_parquet_3 = dest_all_type_parquet_3(spark)
    df_ttination_1_1 = ttination_1_1(spark)
    df_dest_all_type_parquet_4 = dest_all_type_parquet_4(spark)
    df_dest_all_type_parquet_8 = dest_all_type_parquet_8(spark)
    df_two_thousand_rows_table_1 = two_thousand_rows_table_1(spark)
    df_dest_all_type_parquet_2 = dest_all_type_parquet_2(spark)
    df_dest_all_type_parquet_5 = dest_all_type_parquet_5(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/bug_testing_environment_tab")
    
    MetricsCollector.start(
        spark = spark,
        pipelineId = spark.conf.get("prophecy.project.id") + "/" + "pipelines/bug_testing_environment_tab"
    )
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
