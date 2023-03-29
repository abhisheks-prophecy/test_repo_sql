from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from python_pipeline_config.config.ConfigStore import *
from python_pipeline_config.udfs.UDFs import *
from prophecy.utils import *
from python_pipeline_config.graph import *

def pipeline(spark: SparkSession) -> None:
    df_dataset_cust_in = dataset_cust_in(spark)
    df_Reformat_1 = Reformat_1(spark, df_dataset_cust_in)
    df_Subgraph_1 = Subgraph_1(spark, Config.Subgraph_1, df_Reformat_1)
    df_PYTHON_SG = PYTHON_SG(spark, Config.PYTHON_SG, df_Reformat_1)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/PYTHON_PIPELINE_CONFIG")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/PYTHON_PIPELINE_CONFIG")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
