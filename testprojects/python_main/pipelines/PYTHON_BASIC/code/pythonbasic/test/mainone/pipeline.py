from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pythonbasic.test.mainone.config.ConfigStore import *
from pythonbasic.test.mainone.udfs.UDFs import *
from prophecy.utils import *
from pythonbasic.test.mainone.graph import *

def pipeline(spark: SparkSession) -> None:
    df_src_parquet_all_type_and_partition_withspacehyphens = src_parquet_all_type_and_partition_withspacehyphens(spark)
    df_src_parquet_all_type_and_partition_withspacehyphens = collectMetrics(
        spark, 
        df_src_parquet_all_type_and_partition_withspacehyphens, 
        "graph", 
        "vlcUd7KAM_zvifvMwu9pZ$$8MswNyzCALGAeJjSxRNDZ", 
        "CHZEgFOxU2vx1Q32ysboc$$LB9injlM6BGINFq1djzRE"
    )
    df_Reformat_1 = Reformat_1(spark, df_src_parquet_all_type_and_partition_withspacehyphens)
    df_Reformat_1 = collectMetrics(
        spark, 
        df_Reformat_1, 
        "graph", 
        "oSnCy6FWydBV675_CTMv8$$In5C4VfAlHsx5qLzItDuW", 
        "OgcPwunAgx_Y_Y3m8Krd0$$2IcX9S_gny3CS3TcShBZe"
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/PYTHON_BASIC")
    
    MetricsCollector.start(
        spark = spark,
        pipelineId = spark.conf.get("prophecy.project.id") + "/" + "pipelines/PYTHON_BASIC"
    )
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
