from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from python_livy.config.ConfigStore import *
from python_livy.udfs.UDFs import *
from prophecy.utils import *
from python_livy.graph import *

def pipeline(spark: SparkSession) -> None:
    df_src_livy_csv = src_livy_csv(spark)
    df_Reformat_1 = Reformat_1(spark, df_src_livy_csv)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/PYTHON_LIVY")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/PYTHON_LIVY")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
