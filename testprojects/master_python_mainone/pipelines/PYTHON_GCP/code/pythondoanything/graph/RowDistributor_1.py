from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pythondoanything.config.ConfigStore import *
from pythondoanything.udfs.UDFs import *

def RowDistributor_1(spark: SparkSession, in0: DataFrame) -> (DataFrame, DataFrame):
    df1 = in0.filter(((col("`c  float`") < lit(5)) & (col("`c-int-column type`") > lit(Config.c_85))))
    df2 = in0.filter(expr(Config.c_rd_expr))

    return df1, df2
