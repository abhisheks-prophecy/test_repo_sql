from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from livy_python.config.ConfigStore import *
from livy_python.udfs.UDFs import *
from prophecy.utils import *
from livy_python.graph import *

def pipeline(spark: SparkSession) -> None:
    df_livy_src_csv_py = livy_src_csv_py(spark)
    df_livy_src_csv_py = collectMetrics(
        spark, 
        df_livy_src_csv_py, 
        "graph", 
        "zx05CFL6hqZyBtPOu1pkf$$lpZkqTQg5wA92sceBzvlb", 
        "s1Tt_dqrlegFG6g3Zv_GA$$HiT49iNoOhQUhnT5w6ngO"
    )
    Lookup_1(spark, df_livy_src_csv_py)
    df_Reformat_1 = Reformat_1(spark, df_livy_src_csv_py)
    df_Reformat_1 = collectMetrics(
        spark, 
        df_Reformat_1, 
        "graph", 
        "LmVB9Tiv7gOksXKYvwwqv$$vRQhC-glg7Tz8vdDjYpL9", 
        "sEqrqRgL-8RgpQvmEL4qv$$oEdp_cRkhh3jJCtYi3j2w"
    )
    df_Subgraph_4 = Subgraph_4(spark, df_livy_src_csv_py)
    df_RowDistributor_1_out0, df_RowDistributor_1_out1 = RowDistributor_1(spark, df_Subgraph_4)
    df_RowDistributor_1_out0 = collectMetrics(
        spark, 
        df_RowDistributor_1_out0, 
        "graph", 
        "okdX0HxsByJytl8-bE0Kt$$lnNRIoVu9tZHpCufDrTEZ", 
        "FbZ5yqx70ILvxmoLM6e4Z$$C0Xl-mX0aCzxIHmVk8rhU"
    )
    df_RowDistributor_1_out1 = collectMetrics(
        spark, 
        df_RowDistributor_1_out1, 
        "graph", 
        "okdX0HxsByJytl8-bE0Kt$$lnNRIoVu9tZHpCufDrTEZ", 
        "2vqZcM6pio0AhP0eYPP7p$$fpPmls3odio2P4nWrxi_9"
    )
    df_SetOperation_1 = SetOperation_1(spark, df_Reformat_1, df_Reformat_1)
    df_SetOperation_1 = collectMetrics(
        spark, 
        df_SetOperation_1, 
        "graph", 
        "ZZMLx6qIPkco-6Lzn_Bkq$$W6J4G5WzbXaK0Ev-YvUcS", 
        "ISaF3Q4I-FudtVSXTdXHt$$6W-4kvVqiAX0YhO5TKPng"
    )
    df_pythonLivySG1_1 = pythonLivySG1_1(spark, df_SetOperation_1)
    df_Script_1 = Script_1(spark, df_pythonLivySG1_1)
    df_Script_1 = collectMetrics(
        spark, 
        df_Script_1, 
        "graph", 
        "J--b-oIjydkxvRor0nfJk$$zRFrI8FeENs7PJUBEUZkj", 
        "6P3wEhwnkqn3Ru_HerSj_$$pZ6MXoP5vFpLhG5YXvMyP"
    )
    df_Reformat_3 = Reformat_3(spark, df_Script_1)
    df_Reformat_3 = collectMetrics(
        spark, 
        df_Reformat_3, 
        "graph", 
        "Jxv_Oyc8PQrWG1PmyrJWU$$NO49MgMTi0uBbmNDIOcvb", 
        "wWK-RoSVQWQkdaGB7wCwl$$gnAu9To4IOgvRScWdZu07"
    )
    df_Reformat_3.cache().count()
    df_Reformat_3.unpersist()
    df_Reformat_2 = Reformat_2(spark, df_RowDistributor_1_out0)
    df_Reformat_2 = collectMetrics(
        spark, 
        df_Reformat_2, 
        "graph", 
        "-JMYTUou3YiFhB2tmOsc2$$8GHDhvfsV8g3p9540HDNx", 
        "KOVVuemi6iWTgEkkqxTOu$$sRJ676Y739CsKUd1s-dI6"
    )
    df_SchemaTransform_1 = SchemaTransform_1(spark, df_Reformat_2)
    df_SchemaTransform_1 = collectMetrics(
        spark, 
        df_SchemaTransform_1, 
        "graph", 
        "O2L_X7boFoR4lSoQnpI6s$$cm5YF5lbAABPT2sbudYnJ", 
        "dWkzwBE-t4wT5V0o2eDHv$$aSn9WPFdimLtEtO7-OgrZ"
    )
    df_SchemaTransform_1.cache().count()
    df_SchemaTransform_1.unpersist()
    df_Filter_2 = Filter_2(spark, df_RowDistributor_1_out1)
    df_Filter_2 = collectMetrics(
        spark, 
        df_Filter_2, 
        "graph", 
        "XNaycAFAbJ4hInFkYL4az$$tQzPHz9quEmQPMtKjyRJc", 
        "DSV6Pj9Gmo2SH1NbPcct3$$ZSJEXSajOByNasLwgdWwH"
    )
    df_Filter_2.cache().count()
    df_Filter_2.unpersist()

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/LIVY_PYTHON")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/LIVY_PYTHON")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
