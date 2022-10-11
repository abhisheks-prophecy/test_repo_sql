from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from livy_python.config.ConfigStore import *
from livy_python.udfs.UDFs import *
from prophecy.utils import *
from livy_python.graph import *

def pipeline(spark: SparkSession) -> None:
    df_livy_src_csv_py = livy_src_csv_py(spark)
    Lookup_1(spark, df_livy_src_csv_py)
    df_Reformat_1 = Reformat_1(spark, df_livy_src_csv_py)
    df_Subgraph_4 = Subgraph_4(spark, df_livy_src_csv_py)
    df_RowDistributor_1_out0, df_RowDistributor_1_out1 = RowDistributor_1(spark, df_Subgraph_4)
    df_SetOperation_1 = SetOperation_1(spark, df_Reformat_1, df_Reformat_1)
    df_pythonLivySG1_1 = pythonLivySG1_1(spark, df_SetOperation_1)
    df_Script_1 = Script_1(spark, df_pythonLivySG1_1)
    df_Reformat_3 = Reformat_3(spark, df_Script_1)
    df_Reformat_2 = Reformat_2(spark, df_RowDistributor_1_out0)
    df_SchemaTransform_1 = SchemaTransform_1(spark, df_Reformat_2)
    df_Filter_2 = Filter_2(spark, df_RowDistributor_1_out1)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/LIVY_PYTHON")
    
    MetricsCollector.start(
        spark = spark,
        pipelineId = spark.conf.get("prophecy.project.id") + "/" + "pipelines/LIVY_PYTHON"
    )
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
