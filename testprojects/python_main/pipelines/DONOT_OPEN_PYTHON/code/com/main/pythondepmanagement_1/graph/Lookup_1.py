from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from com.main.pythondepmanagement_1.config.ConfigStore import *
from com.main.pythondepmanagement_1.udfs.UDFs import *

def Lookup_1(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''c- short''', '''c  - int''']
    valueColumns = ['''c-string''']
    createLookup("Lookup_1", in0, spark, keyColumns, valueColumns)
