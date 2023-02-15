from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from com.main1.pythondepmanagement_1.config.ConfigStore import *
from com.main1.pythondepmanagement_1.udfs.UDFs import *

def Filter_1_1_1_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("cmls_acct_cobrnd_bus_id_drvd") > lit(-10)))
