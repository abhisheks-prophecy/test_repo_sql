from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.lookups import (
    createLookup,
    createRangeLookup,
    lookup,
    lookup_last,
    lookup_match,
    lookup_count,
    lookup_row,
    lookup_row_reverse,
    lookup_nth
)
a = 10

def registerUDFs(spark: SparkSession):
    spark.udf.register("udf1", udf1)

@udf(returnType = IntegerType())
def udf1(value):
    return value * a if value != None else a * a
