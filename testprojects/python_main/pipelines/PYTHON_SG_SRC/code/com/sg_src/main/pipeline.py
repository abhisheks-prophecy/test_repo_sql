from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from com.sg_src.main.config.ConfigStore import *
from com.sg_src.main.udfs.UDFs import *
from prophecy.utils import *
from com.sg_src.main.graph import *

def pipeline(spark: SparkSession) -> None:
    df_src_parquet_all_type_and_partition_withspacehyphens = src_parquet_all_type_and_partition_withspacehyphens(spark)
    df_src_parquet_all_type_and_partition_withspacehyphens = collectMetrics(
        spark, 
        df_src_parquet_all_type_and_partition_withspacehyphens, 
        "graph", 
        "8CMZjneZeAtyULAaSgjr7$$YgEIbHI5lKFjLFPVmJ0U4", 
        "nWXM5TsGqqk8hm3T4uB59$$AhXAXszhxoAva5XFdTZIY"
    )
    df_Subgraph_1_out0, df_Subgraph_1_out1, df_Subgraph_1_out2 = Subgraph_1(
        spark, 
        df_src_parquet_all_type_and_partition_withspacehyphens, 
        df_src_parquet_all_type_and_partition_withspacehyphens, 
        df_src_parquet_all_type_and_partition_withspacehyphens
    )
    df_Subgraph_1_out0.cache().count()
    df_Subgraph_1_out0.unpersist()
    df_Subgraph_1_out1.cache().count()
    df_Subgraph_1_out1.unpersist()
    df_Subgraph_1_out2.cache().count()
    df_Subgraph_1_out2.unpersist()
    df_Reformat_1 = Reformat_1(spark, df_src_parquet_all_type_and_partition_withspacehyphens)
    df_Reformat_1 = collectMetrics(
        spark, 
        df_Reformat_1, 
        "graph", 
        "yXWP_yM8556OagLh0CNOg$$2H9K5gi0LQUkeSpXL-tDS", 
        "Bx0xKNn6finRJcQvaLRGi$$VbQp1a1kyPW8vcDGuO7pG"
    )
    df_Reformat_1.cache().count()
    df_Reformat_1.unpersist()

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    MetricsCollector.initializeMetrics(spark)
    spark.conf.set("prophecy.collect.basic.stats", "true")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    spark.conf.set("spark.sql.optimizer.excludedRules", "org.apache.spark.sql.catalyst.optimizer.ColumnPruning")
    spark.conf.set("prophecy.metadata.pipeline.uri", "7235/pipelines/PYTHON_SG_SRC")
    MetricsCollector.start(spark = spark, pipelineId = "7235/pipelines/PYTHON_SG_SRC")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
