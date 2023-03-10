from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pythonbasic.test.mainone.config.ConfigStore import *
from pythonbasic.test.mainone.udfs.UDFs import *
from prophecy.utils import *
from pythonbasic.test.mainone.graph import *

def pipeline(spark: SparkSession) -> None:
    df_src_csv_special_char_column_name = src_csv_special_char_column_name(spark)
    df_Reformat_1 = Reformat_1(spark, df_src_csv_special_char_column_name)
    df_Script_4 = Script_4(spark, df_Reformat_1)
    df_Script_5 = Script_5(spark, df_src_csv_special_char_column_name)
    df_Reformat_3 = Reformat_3(spark, df_Script_5)
    dest_csv_py_io_only(spark, df_Reformat_1)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/EM_DISABLED_PYTHON_BASIC")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/EM_DISABLED_PYTHON_BASIC")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
