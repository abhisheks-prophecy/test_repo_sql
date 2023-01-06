from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from com.main1.pythondepmanagement_1.config.ConfigStore import *
from com.main1.pythondepmanagement_1.udfs.UDFs import *

def RowDistributor_1_1_1_1(spark: SparkSession, in0: DataFrame) -> (DataFrame, DataFrame):
    df1 = in0.filter((col("cmls_acct_cobrnd_bus_id_drvd") > lit(-1)))
    df2 = in0.filter((col("cmls_acct_num_iso_bin_num_drvd") > lit(-10)))

    return df1, df2
