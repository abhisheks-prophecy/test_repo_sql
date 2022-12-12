from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from org.donot.openme12.config.ConfigStore import *
from org.donot.openme12.udfs.UDFs import *

def RowDistributor_1_1_1(spark: SparkSession, in0: DataFrame) -> (DataFrame, DataFrame):
    df1 = in0.filter((col("`c- short`") > lit(-1)))
    df2 = in0.filter((col("`c- short`") > lit(5)))

    return df1, df2