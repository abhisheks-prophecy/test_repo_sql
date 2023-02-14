from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from streamingallpipeline.streaming.main.one.this.config.ConfigStore import *
from streamingallpipeline.streaming.main.one.this.udfs.UDFs import *
from prophecy.utils import *
from streamingallpipeline.streaming.main.one.this.graph import *

def pipeline(spark: SparkSession) -> None:
    df_ParquetStandard = ParquetStandard(spark)
    df_Deduplicate_1 = Deduplicate_1(spark, df_ParquetStandard)
    df_Reformat_3 = Reformat_3(spark, df_Deduplicate_1)
    df_Filter_2 = Filter_2(spark, df_Reformat_3)
    df_SetOperation_2 = SetOperation_2(spark, df_Filter_2, df_Filter_2)
    df_RowDistributor_1_out0, df_RowDistributor_1_out1 = RowDistributor_1(spark, df_SetOperation_2)
    df_Limit_1 = Limit_1(spark, df_RowDistributor_1_out1)
    df_Join_2 = Join_2(spark, df_Limit_1, df_Limit_1)
    df_Repartition_1 = Repartition_1(spark, df_Join_2)
    df_SQLStatement_1 = SQLStatement_1(spark, df_Repartition_1)
    df_Script_1 = Script_1(spark, df_SQLStatement_1)
    ParquetTarget(spark, df_Script_1)
    df_SQSSource = SQSSource(spark)
    df_Reformat_2 = Reformat_2(spark, df_SQSSource)
    df_DELTASource = DELTASource(spark)
    df_Reformat_1 = Reformat_1(spark, df_DELTASource)
    DeltaTarget(spark, df_Reformat_1)
    StreamingTarget_1(spark, df_Reformat_2)
    df_SchemaTransform_1 = SchemaTransform_1(spark, df_RowDistributor_1_out0)
    df_CSVAutoloader = CSVAutoloader(spark)
    df_Join_1 = Join_1(spark, df_CSVAutoloader, df_CSVAutoloader)
    CSVTarget(spark, df_Join_1)
    df_FlattenSchema_1 = FlattenSchema_1(spark, df_SchemaTransform_1)
    StreamingTarget_2(spark, df_FlattenSchema_1)
    df_JSONAutoloader = JSONAutoloader(spark)
    df_Filter_1 = Filter_1(spark, df_JSONAutoloader)
    df_ORCStandard = ORCStandard(spark)
    df_SetOperation_1 = SetOperation_1(spark, df_ORCStandard, df_ORCStandard)
    ORCTarget(spark, df_SetOperation_1)
    JSONTarget(spark, df_Filter_1)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("fs.s3a.secret.key", "6oy7IXWucG7WcOSSM3fzlqAY1UafKYqFd7zlQi9s")
    spark.conf.set("fs.s3a.access.key", "AKIAR6ESAR2JAQNZNVMH")
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/StreamingAllPipeline")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAR6ESAR2JAQNZNVMH")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/StreamingAllPipeline")
    pipeline(spark)
    
    spark.streams.resetTerminated()
    spark.streams.awaitAnyTermination()
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
