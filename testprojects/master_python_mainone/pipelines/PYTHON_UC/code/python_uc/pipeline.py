from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from python_uc.config.ConfigStore import *
from python_uc.udfs.UDFs import *
from prophecy.utils import *
from python_uc.graph import *

def pipeline(spark: SparkSession) -> None:
    df_Script_4 = Script_4(spark)
    df_Script_10 = Script_10(spark, df_Script_4)
    df_Script_10_1 = Script_10_1(spark, df_Script_10)
    df_Script_10_1_1 = Script_10_1_1(spark, df_Script_10_1)
    df_Script_10_1_1_1 = Script_10_1_1_1(spark, df_Script_10_1_1)
    df_Script_10_1_1_1_1 = Script_10_1_1_1_1(spark, df_Script_10_1_1_1)
    df_Script_10_1_1_1_1_1_1_1_1_1_1_out0, df_Script_10_1_1_1_1_1_1_1_1_1_1_output1 = Script_10_1_1_1_1_1_1_1_1_1_1(
        spark, 
        df_Script_10_1_1_1
    )
    df_Script_10_1_1_1_1_1_1_1_1_1_1_1_out0, df_Script_10_1_1_1_1_1_1_1_1_1_1_1_output1 = Script_10_1_1_1_1_1_1_1_1_1_1_1(
        spark, 
        df_Script_10_1_1_1_1_1_1_1_1_1_1_output1
    )
    df_Script_10_1_1_1_1_1_1_1_1_1_1_1_1_out0, df_Script_10_1_1_1_1_1_1_1_1_1_1_1_1_output1 = Script_10_1_1_1_1_1_1_1_1_1_1_1_1(
        spark, 
        df_Script_10_1_1_1_1_1_1_1_1_1_1_1_out0
    )
    df_Script_10_1_1_1_1_1 = Script_10_1_1_1_1_1(spark, df_Script_10_1_1_1_1)
    df_Script_10_1_1_1_1_1_1 = Script_10_1_1_1_1_1_1(spark, df_Script_10_1_1_1_1_1)
    df_Script_10_1_1_1_1_1_1_1 = Script_10_1_1_1_1_1_1_1(spark, df_Script_10_1_1_1_1_1_1)
    df_Script_10_1_1_1_1_1_1_1_1_output0, df_Script_10_1_1_1_1_1_1_1_1_output1, df_Script_10_1_1_1_1_1_1_1_1_output2 = Script_10_1_1_1_1_1_1_1_1(
        spark, 
        df_Script_10_1_1_1_1_1_1_1
    )
    df_Script_10_1_1_1_1_1_1_1_1_1 = Script_10_1_1_1_1_1_1_1_1_1(spark, df_Script_10_1_1_1_1_1_1_1_1_output0)
    df_all_type_parquet = all_type_parquet(spark)
    df_WithUDFConfig = WithUDFConfig(spark, df_all_type_parquet)
    df_table_with_read_permission_for_user = table_with_read_permission_for_user(spark)
    df_Reformat_6 = Reformat_6(spark, df_table_with_read_permission_for_user)
    df_src_parquet_unity_catalog = src_parquet_unity_catalog(spark)
    df_Reformat_1 = Reformat_1(spark, df_src_parquet_unity_catalog)
    df_OrderBy_1 = OrderBy_1(spark, df_Reformat_1)
    df_Reformat_2 = Reformat_2(spark, df_WithUDFConfig)
    df_Filter_1 = Filter_1(spark, df_Reformat_2)
    df_SQLStatement_1_1_out, df_SQLStatement_1_1_out1 = SQLStatement_1_1(spark, df_Reformat_1, df_Reformat_1)
    dest_uc_out2(spark, df_SQLStatement_1_1_out)
    df_Join_1 = Join_1(spark, df_Filter_1, df_Filter_1)
    df_FlattenSchema_1 = FlattenSchema_1(spark, df_all_type_parquet)
    df_SQLSte_out, df_SQLSte_out1 = SQLSte(spark, df_FlattenSchema_1, df_WithUDFConfig)
    df_Limit_1 = Limit_1(spark, df_Script_10_1_1_1_1_1_1)
    df_Script_11 = Script_11(spark, df_Script_10_1_1_1_1_1_1_1_1_output1)
    df_Subgraph_1_out0, df_Subgraph_1_out1 = Subgraph_1(
        spark, 
        df_src_parquet_unity_catalog, 
        df_src_parquet_unity_catalog
    )
    df_Aggregate_1 = Aggregate_1(spark, df_OrderBy_1)
    df_Filter_3 = Filter_3(spark, df_Script_10_1)
    df_Script_1 = Script_1(spark, df_Filter_1)
    df_WindowFunction_1 = WindowFunction_1(spark, df_OrderBy_1)
    df_Reformat_5 = Reformat_5(spark, df_Script_10_1_1_1_1)
    df_Script_12 = Script_12(spark, df_Script_10_1_1_1_1_1_1_1)
    dest_uc_target(spark, df_Filter_1)
    df_Deduplicate_1 = Deduplicate_1(spark, df_Aggregate_1)
    df_RowDistributor_1_out0, df_RowDistributor_1_out1, df_RowDistributor_1_out2 = RowDistributor_1(
        spark, 
        df_WithUDFConfig
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/PYTHON_UC")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/PYTHON_UC")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
