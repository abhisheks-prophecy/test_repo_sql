from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from perf_unitest_generate.config.ConfigStore import *
from perf_unitest_generate.udfs.UDFs import *

def Lookup_1(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''account_flags''']
    valueColumns = ['''account_open_date''', '''country_code''']
    createLookup("Lookup1", in0, spark, keyColumns, valueColumns)
