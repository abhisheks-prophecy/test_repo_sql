from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pipeline__102.config.ConfigStore import *
from pipeline__102.udfs.UDFs import *
from prophecy.utils import *
from pipeline__102.graph import *

def pipeline(spark: SparkSession) -> None:
    df_table_number_18845 = table_number_18845(spark)
    df_table_number_18848 = table_number_18848(spark)
    df_table_number_17187 = table_number_17187(spark)
    df_table_number_18844 = table_number_18844(spark)
    df_table_number_18857 = table_number_18857(spark)
    df_table_number_18827 = table_number_18827(spark)
    df_table_number_18854 = table_number_18854(spark)
    df_table_number_18826 = table_number_18826(spark)
    df_table_number_18897 = table_number_18897(spark)
    df_table_number_17182 = table_number_17182(spark)
    df_table_number_17218 = table_number_17218(spark)
    df_table_number_17214 = table_number_17214(spark)
    df_table_number_1001 = table_number_1001(spark)
    df_table_number_17186 = table_number_17186(spark)
    df_table_number_18850 = table_number_18850(spark)
    df_table_number_17217 = table_number_17217(spark)
    df_table_number_18829 = table_number_18829(spark)
    df_table_number_18894 = table_number_18894(spark)
    df_table_number_17227 = table_number_17227(spark)
    df_table_number_18842 = table_number_18842(spark)
    df_table_number_17181 = table_number_17181(spark)
    df_table_number_1883 = table_number_1883(spark)
    df_table_number_10003 = table_number_10003(spark)
    df_table_number_18846 = table_number_18846(spark)
    df_table_number_18828 = table_number_18828(spark)
    df_table_number_18831 = table_number_18831(spark)
    df_table_number_17183 = table_number_17183(spark)
    df_table_number_17220 = table_number_17220(spark)
    df_table_number_18841 = table_number_18841(spark)
    df_table_number_18896 = table_number_18896(spark)
    df_table_number_18858 = table_number_18858(spark)
    df_table_number_17219 = table_number_17219(spark)
    df_table_number_17213 = table_number_17213(spark)
    df_table_number_18855 = table_number_18855(spark)
    df_table_number_17228 = table_number_17228(spark)
    df_table_number_10001 = table_number_10001(spark)
    df_table_number_18843 = table_number_18843(spark)
    df_table_number_1722 = table_number_1722(spark)
    df_table_number_1884 = table_number_1884(spark)
    df_table_number_18836 = table_number_18836(spark)
    df_table_number_18830 = table_number_18830(spark)
    df_table_number_17216 = table_number_17216(spark)
    df_table_number_18849 = table_number_18849(spark)
    df_table_number_1 = table_number_1(spark)
    df_table_number_18835 = table_number_18835(spark)
    df_a_table_with_data_1 = a_table_with_data_1(spark)
    df_table_number_18851 = table_number_18851(spark)
    df_table_number_18834 = table_number_18834(spark)
    df_table_number_18852 = table_number_18852(spark)
    df_table_number_17215 = table_number_17215(spark)
    df_table_number_18838 = table_number_18838(spark)
    df_table_number_17184 = table_number_17184(spark)
    df_table_number_18840 = table_number_18840(spark)
    df_table_number_18856 = table_number_18856(spark)
    df_table_number_18839 = table_number_18839(spark)
    df_table_number_18832 = table_number_18832(spark)
    df_table_number_18833 = table_number_18833(spark)
    df_table_number_18837 = table_number_18837(spark)
    df_table_number_0 = table_number_0(spark)
    df_table_number_17185 = table_number_17185(spark)
    df_table_number_18847 = table_number_18847(spark)
    df_table_number_17212 = table_number_17212(spark)
    df_table_number_18853 = table_number_18853(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/pipeline__102")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/pipeline__102")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
