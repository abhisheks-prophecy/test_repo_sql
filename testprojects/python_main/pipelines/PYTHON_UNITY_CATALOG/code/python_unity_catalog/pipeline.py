from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from python_unity_catalog.config.ConfigStore import *
from python_unity_catalog.udfs.UDFs import *
from prophecy.utils import *
from python_unity_catalog.graph import *

def pipeline(spark: SparkSession) -> None:
    df_src_parquet_unity_catalog = src_parquet_unity_catalog(spark)
    df_all_type_parquet_1 = all_type_parquet_1(spark)
    df_Reformat_1 = Reformat_1(spark, df_src_parquet_unity_catalog)
    df_Reformat_2 = Reformat_2(spark, df_all_type_parquet_1)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/PYTHON_UNITY_CATALOG")
    
    MetricsCollector.start(
        spark = spark,
        pipelineId = spark.conf.get("prophecy.project.id") + "/" + "pipelines/PYTHON_UNITY_CATALOG"
    )
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
