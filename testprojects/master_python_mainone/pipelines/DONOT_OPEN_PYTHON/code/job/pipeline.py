from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *
from prophecy.utils import *
from job.graph import *

def pipeline(spark: SparkSession) -> None:
    df_all_type_part_parquet = all_type_part_parquet(spark)
    df_all_type_part_parquet = collectMetrics(
        spark, 
        df_all_type_part_parquet, 
        "graph", 
        "all_type_part_parquet", 
        "K1XpTR7-jgHPuX6PHVdPr$$wHEmbxozybFo-eRVP11a3"
    )
    df_RowDistributor_1_out0, df_RowDistributor_1_out1, df_RowDistributor_1_out2, df_RowDistributor_1_out3 = RowDistributor_1(
        spark, 
        df_all_type_part_parquet
    )
    df_RowDistributor_1_out0 = collectMetrics(spark, df_RowDistributor_1_out0, "graph", "RowDistributor_1", "out0")
    df_RowDistributor_1_out1 = collectMetrics(
        spark, 
        df_RowDistributor_1_out1, 
        "graph", 
        "RowDistributor_1", 
        "W59iy89iTXSgFaxfQEwhW"
    )
    df_RowDistributor_1_out2 = collectMetrics(
        spark, 
        df_RowDistributor_1_out2, 
        "graph", 
        "RowDistributor_1", 
        "3a-JMPZZW4g-OIK6u8pOH"
    )
    df_RowDistributor_1_out3 = collectMetrics(
        spark, 
        df_RowDistributor_1_out3, 
        "graph", 
        "RowDistributor_1", 
        "CKp-fYcvwRV-xHIE140JV"
    )
    Lookup_1(spark, df_RowDistributor_1_out1)
    df_csv_special_chars = csv_special_chars(spark)
    df_csv_special_chars = collectMetrics(
        spark, 
        df_csv_special_chars, 
        "graph", 
        "csv_special_chars", 
        "ORTrvFWDCkxykwAXnkn1g$$fwpMbWEu626Vg01d_d77R"
    )
    df_Limit_1 = Limit_1(spark, df_csv_special_chars)
    df_Limit_1 = collectMetrics(spark, df_Limit_1, "graph", "Limit_1", "VNcdLoshlAMxxP9q3P6_1$$4ddpNdJqSE6w7cPGroGxj")
    df_Filter_1 = Filter_1(spark, df_Limit_1)
    df_Filter_1 = collectMetrics(
        spark, 
        df_Filter_1, 
        "graph", 
        "Filter_1", 
        "2sVaJyYIOhDWczNzrH865$$EZP0zXynO1ZUGdxYXqVHn"
    )
    df_ConfigAndUDF = ConfigAndUDF(spark, df_Filter_1)
    df_ConfigAndUDF = collectMetrics(
        spark, 
        df_ConfigAndUDF, 
        "graph", 
        "ConfigAndUDF", 
        "0YDYZPDtmMHOdXMO3igze$$uzOCbwhK48a5yijnl6X1B"
    )
    df_OrderBy_1 = OrderBy_1(spark, df_ConfigAndUDF)
    df_OrderBy_1 = collectMetrics(
        spark, 
        df_OrderBy_1, 
        "graph", 
        "OrderBy_1", 
        "XMZ9PL47KCK49bhwywt-X$$ApvHMBHWabBeZQaHanXfh"
    )
    df_Deduplicate_1 = Deduplicate_1(spark, df_OrderBy_1)
    df_Deduplicate_1 = collectMetrics(
        spark, 
        df_Deduplicate_1, 
        "graph", 
        "Deduplicate_1", 
        "x7W-pjW-6TjOG3oAscMiK$$3-swEthJhrqJivBfqlgl_"
    )
    df_SchemaTransform_1 = SchemaTransform_1(spark, df_Deduplicate_1)
    df_SchemaTransform_1 = collectMetrics(
        spark, 
        df_SchemaTransform_1, 
        "graph", 
        "SchemaTransform_1", 
        "9322GqVhXmMJikkf6i0hH$$CjC7KUFPAPleRMaw7mmWn"
    )
    df_SetOperation_1 = SetOperation_1(spark, df_SchemaTransform_1, df_SchemaTransform_1)
    df_SetOperation_1 = collectMetrics(
        spark, 
        df_SetOperation_1, 
        "graph", 
        "SetOperation_1", 
        "WU1M6vXwP42j5xhraZQ3H$$5gnYkvzcJzyHGXzBxtNsd"
    )
    df_csv_all_type = csv_all_type(spark)
    df_csv_all_type = collectMetrics(
        spark, 
        df_csv_all_type, 
        "graph", 
        "csv_all_type", 
        "_posjARhGw9jdalJzj-z7$$evC658R8ZUCPVoEw1Zwo0"
    )
    Target_5(spark, df_csv_all_type)
    df_delta = delta(spark)
    df_delta = collectMetrics(spark, df_delta, "graph", "delta", "m-fvj-aHPWpu0ZMPSFDzn$$pGK30odKzqypWhcv-uIx0")
    df_Limit_4 = Limit_4(spark, df_delta)
    df_Limit_4 = collectMetrics(spark, df_Limit_4, "graph", "Limit_4", "7Jv_nxOWQ3vIMtWiogYvM$$36XNVyAh1tPUEZN2JMuoK")
    df_Join_1 = Join_1(spark, df_all_type_part_parquet, df_all_type_part_parquet)
    df_Join_1 = collectMetrics(spark, df_Join_1, "graph", "Join_1", "skdKHf3bW8p-K6g7nyxDC$$bKroTYjQqC3HbFRuqzfzi")
    df_Limit_7 = Limit_7(spark, df_Join_1)
    df_Limit_7 = collectMetrics(spark, df_Limit_7, "graph", "Limit_7", "i1Nsh_PK_FcaOd_3McZm6$$aLqbOLClxwrjo0T9rUVwW")
    df_Repartition_1 = Repartition_1(spark, df_Limit_7)
    df_Repartition_1 = collectMetrics(
        spark, 
        df_Repartition_1, 
        "graph", 
        "Repartition_1", 
        "yrNcCB_hx8IOjKH2Wh8Jx$$-xZBC0SRBrEmyMsO8m70G"
    )
    df_SubGraph_2 = SubGraph_2(spark, df_Repartition_1, df_RowDistributor_1_out0)
    df_orc_src = orc_src(spark)
    df_orc_src = collectMetrics(spark, df_orc_src, "graph", "orc_src", "44_XPIJCPj1I1tCDMEHmh$$EXLiSwqLDSiRnR3w528in")
    df_Deduplicate_2 = Deduplicate_2(spark, df_orc_src)
    df_Deduplicate_2 = collectMetrics(
        spark, 
        df_Deduplicate_2, 
        "graph", 
        "Deduplicate_2", 
        "olvbl4JfshuFca-YS1IgG$$MwzaZr7NHXWa5wa4BnmUj"
    )
    df_Reformat_5 = Reformat_5(spark, df_Deduplicate_2)
    df_Reformat_5 = collectMetrics(
        spark, 
        df_Reformat_5, 
        "graph", 
        "Reformat_5", 
        "KHVcU5Rq5aJT_x6sZwuKL$$9z7HwEmo_IqtzD5yPwjo5"
    )
    df_custom_xlsx_py = custom_xlsx_py(spark)
    df_custom_xlsx_py = collectMetrics(
        spark, 
        df_custom_xlsx_py, 
        "graph", 
        "custom_xlsx_py", 
        "x7Baf_F5S7tfpGd7r7gW1$$60TmOa0hv75TCPx3e5pls"
    )
    df_Reformat_10 = Reformat_10(spark, df_custom_xlsx_py)
    df_Reformat_10 = collectMetrics(
        spark, 
        df_Reformat_10, 
        "graph", 
        "Reformat_10", 
        "jFYeuI61G65B3chaMcMi9$$hssxGCZ1WSwcXOtiDElJH"
    )
    df_Reformat_10.cache().count()
    df_Reformat_10.unpersist()
    df_Aggregate_1 = Aggregate_1(spark, df_SetOperation_1)
    df_Aggregate_1 = collectMetrics(
        spark, 
        df_Aggregate_1, 
        "graph", 
        "Aggregate_1", 
        "eXuydhO1EqpAMfbq8b8LT$$dqKKSFDwQjydqmvIi4Dft"
    )
    df_all_components_1_out0, df_all_components_1_out1, df_all_components_1_out2 = all_components_1(
        spark, 
        df_all_type_part_parquet, 
        df_all_type_part_parquet, 
        df_all_type_part_parquet
    )
    df_text = text(spark)
    df_text = collectMetrics(spark, df_text, "graph", "text", "gWPrIMMlkrG2UvHqNPj6D$$FruLC9PEQ8DO2qLMBWDKJ")
    df_Reformat_4 = Reformat_4(spark, df_text)
    df_Reformat_4 = collectMetrics(
        spark, 
        df_Reformat_4, 
        "graph", 
        "Reformat_4", 
        "Z7JdWCGM7uYZheCRUgwvh$$_JKl5kg-4-dO6AgxV7k8-"
    )
    df_Script_1 = Script_1(spark, df_Aggregate_1)
    df_Script_1 = collectMetrics(
        spark, 
        df_Script_1, 
        "graph", 
        "Script_1", 
        "ZvgsUJ_9eyK0UXoawagVL$$gD5njuMvJZpys_gRx0vcy"
    )
    df_json_in = json_in(spark)
    df_json_in = collectMetrics(spark, df_json_in, "graph", "json_in", "AkoFj0jmMBshTbQIjr103$$OaRGS-kcQrvnXd_bho8ve")
    df_Reformat_16 = Reformat_16(spark, df_json_in)
    df_Reformat_16 = collectMetrics(
        spark, 
        df_Reformat_16, 
        "graph", 
        "Reformat_16", 
        "NumrPyiY_QXIWM_GXd7Un$$36iPKHFjqehM2767gFFSP"
    )
    df_catalog = catalog(spark)
    df_catalog = collectMetrics(spark, df_catalog, "graph", "catalog", "cY2VcRtlr_Ypjbo2YK6bm$$RAnzMQK3YjV0hNDb-86v_")
    df_Reformat_9 = Reformat_9(spark, df_RowDistributor_1_out2)
    df_Reformat_9 = collectMetrics(
        spark, 
        df_Reformat_9, 
        "graph", 
        "Reformat_9", 
        "GsbGTC9lDwPUI4TRCfxNm$$yFCIAy92G56gcU54tsYp_"
    )
    df_Reformat_9.cache().count()
    df_Reformat_9.unpersist()
    df_Reformat_15 = Reformat_15(spark, df_csv_all_type)
    df_Reformat_15 = collectMetrics(
        spark, 
        df_Reformat_15, 
        "graph", 
        "Reformat_15", 
        "9jXaJTdFUIjZGLj_vc5cy$$lND32pDnRQ4Mh__Dw292O"
    )
    df_OrderBy_4 = OrderBy_4(spark, df_delta)
    df_OrderBy_4 = collectMetrics(
        spark, 
        df_OrderBy_4, 
        "graph", 
        "OrderBy_4", 
        "JPp8waTMEALCxx-RgmBLe$$7-AKbD5Qi60_VRKRZmJ8w"
    )
    df_Limit_6 = Limit_6(spark, df_catalog)
    df_Limit_6 = collectMetrics(spark, df_Limit_6, "graph", "Limit_6", "huZBjTNVhwDZu-dJ8vCLG$$de1GXZu9-Ly9Y6Smlp7Bj")
    df_Script_3 = Script_3(
        spark, 
        df_Limit_6, 
        df_Limit_6, 
        df_OrderBy_4, 
        df_Limit_4, 
        df_Reformat_16, 
        df_Reformat_15, 
        df_OrderBy_1
    )
    df_Script_3 = collectMetrics(
        spark, 
        df_Script_3, 
        "graph", 
        "Script_3", 
        "8NNUyPXeI68kaysRjOSPK$$wsEyecXcd28exUXVvELKN"
    )
    df_Limit_8 = Limit_8(spark, df_SubGraph_2)
    df_Limit_8 = collectMetrics(spark, df_Limit_8, "graph", "Limit_8", "2i3OcF3ZKDVsz86or5b4T$$Hvs_91XxZ8bYvNEaY_gM-")
    df_Reformat_6 = Reformat_6(spark, df_Limit_8)
    df_Reformat_6 = collectMetrics(
        spark, 
        df_Reformat_6, 
        "graph", 
        "Reformat_6", 
        "OMvFDXYs4rbzeNHPH195J$$GhXfyPI0OdcDGgZ8EKjX3"
    )
    df_Join_3 = Join_3(spark, df_Reformat_6, df_Reformat_5)
    df_Join_3 = collectMetrics(spark, df_Join_3, "graph", "Join_3", "jf3PTjvZYdDjCpHgnyHo4$$3K68Ip_CK3ndgPjPUY9OU")
    df_avro = avro(spark)
    df_avro = collectMetrics(spark, df_avro, "graph", "avro", "O7MlD4SsSNAngvEmyO2mL$$EPYlbrlI9CP94b-tBwNsN")
    df_OrderBy_3 = OrderBy_3(spark, df_avro)
    df_OrderBy_3 = collectMetrics(
        spark, 
        df_OrderBy_3, 
        "graph", 
        "OrderBy_3", 
        "23_EUJ5mmZmxkUF_FP06-$$05CM-G0C71N7lAUdf7DdA"
    )
    df_Reformat_7 = Reformat_7(spark, df_OrderBy_3)
    df_Reformat_7 = collectMetrics(
        spark, 
        df_Reformat_7, 
        "graph", 
        "Reformat_7", 
        "kX6osjcARyB_vtVysaBkg$$k2MRDGFfUnWalJzYqYMPk"
    )
    df_Join_4 = Join_4(spark, df_Join_3, df_Reformat_7)
    df_Join_4 = collectMetrics(spark, df_Join_4, "graph", "Join_4", "kDG9ErGZEr6_nUT7EIVl3$$8ycvBvknk6dwqA7MYQ3ch")
    df_Script_6 = Script_6(
        spark, 
        df_all_components_1_out0, 
        df_all_components_1_out1, 
        df_all_components_1_out1, 
        df_all_components_1_out1, 
        df_all_components_1_out2, 
        df_all_components_1_out2, 
        df_all_components_1_out2
    )
    df_Script_6 = collectMetrics(
        spark, 
        df_Script_6, 
        "graph", 
        "Script_6", 
        "EvF8Dow274Ex0OhnayGYM$$i6UX0uzgLjqHwTvtrZnVu"
    )
    df_Reformat_11 = Reformat_11(spark, df_Script_6)
    df_Reformat_11 = collectMetrics(
        spark, 
        df_Reformat_11, 
        "graph", 
        "Reformat_11", 
        "CslDD0qqB0eax70M2UCCZ$$8mL1DtQCpFdTWxsvaY3xk"
    )
    df_Reformat_11.cache().count()
    df_Reformat_11.unpersist()
    df_Limit_3 = Limit_3(spark, df_Script_1)
    df_Limit_3 = collectMetrics(spark, df_Limit_3, "graph", "Limit_3", "jRQ-hDJGF8airJSOEC9CK$$d49_2pfAycCRBy2dk2ro4")
    df_Limit_3.cache().count()
    df_Limit_3.unpersist()
    df_SubGraph_7 = SubGraph_7(spark, df_Script_3)
    df_Limit_2 = Limit_2(spark, df_SubGraph_7)
    df_Limit_2 = collectMetrics(spark, df_Limit_2, "graph", "Limit_2", "CoKMTNCib_GoZndahl6ba$$Z1Z2zektg1WpYFnHzT1UB")
    df_Limit_2.cache().count()
    df_Limit_2.unpersist()
    df_Source_1 = Source_1(spark)
    df_Source_1 = collectMetrics(
        spark, 
        df_Source_1, 
        "graph", 
        "Source_1", 
        "gC-8YSjj5-83Mi4jzyWeE$$eWVZk_fAShNrHFWsAWIsA"
    )
    df_Limit_9 = Limit_9(spark, df_Join_4)
    df_Limit_9 = collectMetrics(spark, df_Limit_9, "graph", "Limit_9", "0GItvH4CK7tzzNvabLTsh$$r7w5ecu0XsEojWY08vimA")
    df_WindowFunction_1 = WindowFunction_1(spark, df_RowDistributor_1_out3)
    df_WindowFunction_1 = collectMetrics(
        spark, 
        df_WindowFunction_1, 
        "graph", 
        "WindowFunction_1", 
        "THZf4U0Afr0WwFlHIExkT$$ZcG-fixeZPUAPkqvmsgpR"
    )
    Script_2(spark, df_WindowFunction_1)
    df_ComplexExpr = ComplexExpr(spark, df_Source_1)
    df_ComplexExpr = collectMetrics(
        spark, 
        df_ComplexExpr, 
        "graph", 
        "ComplexExpr", 
        "QeW6AXzdAfKrFZuBLp-aB$$4TUqZXq0hzrXa7yNQJNBJ"
    )
    df_OrderBy_5 = OrderBy_5(spark, df_ComplexExpr)
    df_OrderBy_5 = collectMetrics(
        spark, 
        df_OrderBy_5, 
        "graph", 
        "OrderBy_5", 
        "Czcyk9sHItA4nU32spgaq$$qKeZEqV15XoqnCgXV2j2K"
    )
    df_Limit_10 = Limit_10(spark, df_OrderBy_5)
    df_Limit_10 = collectMetrics(
        spark, 
        df_Limit_10, 
        "graph", 
        "Limit_10", 
        "ZC8SywT2cNRc4oLkXKR_e$$NHhRk0hgQU5bh0-_ZZDiB"
    )
    df_Limit_10.cache().count()
    df_Limit_10.unpersist()
    df_Join_5 = Join_5(spark, df_Limit_9, df_Reformat_4)
    df_Join_5 = collectMetrics(spark, df_Join_5, "graph", "Join_5", "dvt3l9wKZJwxnYzggRdLH$$qFCG8vzzPdKTat2OjHZWH")
    df_Join_5.cache().count()
    df_Join_5.unpersist()

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
    spark.conf.set("spark_config1", "spark_config1 value")
    spark.conf.set("spark_config2", "spark_config2 value")
    spark.conf.set("prophecy.metadata.pipeline.uri", "6154/pipelines/DONOT_OPEN_PYTHON")
    spark.sparkContext._jsc.hadoopConfiguration().set("hadoop_config1", "hadoop_config1 value")
    spark.sparkContext._jsc.hadoopConfiguration().set("hadoop_config2", "hadoop_config2 value")
    MetricsCollector.start(spark = spark, pipelineId = "6154/pipelines/DONOT_OPEN_PYTHON")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
