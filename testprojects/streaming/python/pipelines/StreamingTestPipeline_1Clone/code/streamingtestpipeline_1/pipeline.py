from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from streamingtestpipeline_1.config.ConfigStore import *
from streamingtestpipeline_1.udfs.UDFs import *
from prophecy.utils import *
from streamingtestpipeline_1.graph import *

def pipeline(spark: SparkSession) -> None:
    df_CSVSource = CSVSource(spark)
    df_Limit_1 = Limit_1(spark, df_CSVSource)
    CSVTarget(spark, df_Limit_1)
    df_JSONSource = JSONSource(spark)
    df_SetOperation_1 = SetOperation_1(spark, df_JSONSource, df_JSONSource)
    df_ORCSource = ORCSource(spark)
    df_Filter_1 = Filter_1(spark, df_ORCSource)
    ORCTarget(spark, df_Filter_1)
    df_ParquetSrc = ParquetSrc(spark)
    df_Aggregate_1 = Aggregate_1(spark, df_JSONSource)
    df_WindowFunction_1 = WindowFunction_1(spark, df_Aggregate_1)
    df_FlattenSchema_1 = FlattenSchema_1(spark, df_JSONSource)
    df_DeltaSource = DeltaSource(spark)
    df_Limit_2 = Limit_2(spark, df_DeltaSource)
    df_SchemaTransform_1 = SchemaTransform_1(spark, df_SetOperation_1)
    JSONTarget(spark, df_SchemaTransform_1)
    df_Reformat_1 = Reformat_1(spark, df_ParquetSrc)
    ParquetTarget(spark, df_Reformat_1)
    DeltaTarget(spark, df_Limit_2)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/StreamingTestPipeline_1Clone")
    
    MetricsCollector.start(
        spark = spark,
        pipelineId = spark.conf.get("prophecy.project.id") + "/" + "pipelines/StreamingTestPipeline_1Clone"
    )
    pipeline(spark)
    
    spark.streams.resetTerminated()
    spark.streams.awaitAnyTermination()
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
