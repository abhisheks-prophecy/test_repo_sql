from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from livy_python.config.ConfigStore import *
from livy_python.udfs.UDFs import *

def Lookup_1(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''variable''']
    valueColumns = ['''value''', '''unit''']
    createLookup("Lookup1", in0, spark, keyColumns, valueColumns)
