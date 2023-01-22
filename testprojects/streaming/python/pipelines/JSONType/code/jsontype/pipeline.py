from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jsontype.config.ConfigStore import *
from jsontype.udfs.UDFs import *
from prophecy.utils import *
from jsontype.graph import *

def pipeline(spark: SparkSession) -> None:
    df_JSONSource = JSONSource(spark)
    df_Aggregate_1 = Aggregate_1(spark, df_JSONSource)
    df_SetOperation_1 = SetOperation_1(spark, df_JSONSource, df_JSONSource)
    df_SchemaTransform_1 = SchemaTransform_1(spark, df_SetOperation_1)
    JSONTarget(spark, df_SchemaTransform_1)
    df_WindowFunction_1 = WindowFunction_1(spark, df_Aggregate_1)
    df_FlattenSchema_1 = FlattenSchema_1(spark, df_JSONSource)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JSONType")
    
    MetricsCollector.start(spark = spark, pipelineId = spark.conf.get("prophecy.project.id") + "/" + "pipelines/JSONType")
    pipeline(spark)
    
    spark.streams.resetTerminated()
    spark.streams.awaitAnyTermination()
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
