from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from streamingpipelinemain.config.ConfigStore import *
from streamingpipelinemain.udfs.UDFs import *
from prophecy.utils import *
from streamingpipelinemain.graph import *

def pipeline(spark: SparkSession) -> None:
    df_ParquetStandard = ParquetStandard(spark)
    df_Deduplicate_1 = Deduplicate_1(spark, df_ParquetStandard)
    df_Reformat_3 = Reformat_3(spark, df_Deduplicate_1)
    df_Filter_2 = Filter_2(spark, df_Reformat_3)
    df_SetOperation_2 = SetOperation_2(spark, df_Filter_2, df_Filter_2)
    df_RowDistributor_1_out0, df_RowDistributor_1_out1 = RowDistributor_1(spark, df_SetOperation_2)
    df_SchemaTransform_1 = SchemaTransform_1(spark, df_RowDistributor_1_out0)
    df_FlattenSchema_1 = FlattenSchema_1(spark, df_SchemaTransform_1)
    ParquetTarget2(spark, df_FlattenSchema_1)
    df_src_parquet_all_type_and_partition_withspacehyphens_1 = src_parquet_all_type_and_partition_withspacehyphens_1(
        spark
    )
    df_all_components_main_1_out0, df_all_components_main_1_out1, df_all_components_main_1_out2 = all_components_main_1(
        spark, 
        df_src_parquet_all_type_and_partition_withspacehyphens_1, 
        df_src_parquet_all_type_and_partition_withspacehyphens_1, 
        df_src_parquet_all_type_and_partition_withspacehyphens_1
    )
    df_Limit_1 = Limit_1(spark, df_RowDistributor_1_out1)
    df_Join_2 = Join_2(spark, df_Limit_1, df_Limit_1)
    df_Repartition_1 = Repartition_1(spark, df_Join_2)
    df_SQLStatement_1 = SQLStatement_1(spark, df_Repartition_1)
    df_Script_1 = Script_1(spark, df_SQLStatement_1)
    ParquetTarget(spark, df_Script_1)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("fs.s3a.access.key", "AKIAR6ESAR2JAQNZNVMH")
    spark.conf.set("fs.s3a.secret.key", "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")
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/StreamingPipelineMain")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAR6ESAR2JAQNZNVMH")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/StreamingPipelineMain")
    pipeline(spark)
    
    spark.streams.resetTerminated()
    spark.streams.awaitAnyTermination()
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
