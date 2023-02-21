from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *
from prophecy.utils import *
from job.graph import *

def pipeline(spark: SparkSession) -> None:
    df_Source_0 = Source_0(spark)
    df_AllComps_out0, df_AllComps_out1, df_AllComps_out2 = AllComps(spark, df_Source_0, df_Source_0, df_Source_0)
    df_RecSG = RecSG(spark, df_Source_0)
    df_MultiPort_out0, df_MultiPort_out1, df_MultiPort_out2, df_MultiPort_out3 = MultiPort(
        spark, 
        df_Source_0, 
        df_Source_0, 
        df_Source_0, 
        df_Source_0
    )

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/PYTHON_SUBGRAPH_CREATOR")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/PYTHON_SUBGRAPH_CREATOR")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
