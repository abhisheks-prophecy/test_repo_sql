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
initial = 10

def registerUDFs(spark: SparkSession):
    spark.udf.register("squared", squared)
    spark.udf.register("factorial", factorial)
    spark.udf.register("random_string", random_string)
    spark.udf.register("udf_scipy_dependency", udf_scipy_dependency)
    spark.udf.register("udf_swap_product", udf_swap_product)
    spark.udf.register("udf_prime", udf_prime)
    spark.udf.register("udf_datetype", udf_datetype)
    spark.udf.register("udf_timestamptype", udf_timestamptype)
    spark.udf.register("udf_arraytype", udf_arraytype)
    spark.udf.register("udf_maptype", udf_maptype)
    spark.udf.register("udf_tokenize", udf_tokenize)

@udf(returnType = IntegerType())
def squared(input):
    input = int(input) if input is not None else 2

    return int(input) * int(input) * initial if input is not None else initial

@udf(returnType = IntegerType())
def factorial(input):
    input = int(input) if input is not None else 2

    return int(input) * int(input) if input is not None else initial

@udf(returnType = StringType())
def random_string(length, extra_characters=""):
    import string, random
    length = int(length) if length is not None else 2

    return "".join(
        [
          random.choice(string.ascii_lowercase + string.ascii_uppercase + string.digits + extra_characters)
          for _ in range(length)
        ]
    )

@udf(returnType = StringType())
def udf_scipy_dependency():
    from scipy.special import cbrt
    cb = cbrt([27, 64])

    return str(cb[0])

@udf(returnType = IntegerType())
def udf_swap_product(x, y):
    x = x ^ y
    y = x ^ y
    x = x ^ y

    return x * y

@udf(returnType = BooleanType())
def udf_prime(x):
    if num > 1:
        # check for factors
        for i in range(2, num):
            if (num % i) == 0:
                return False
        else:
            return True
    else:
        return False

@udf(returnType = DateType())
def udf_datetype(value):
    return to_date(value)

@udf(returnType = TimestampType())
def udf_timestamptype(value):
    return to_timestamp(value)

@udf(returnType = ArrayType(IntegerType()))
def udf_arraytype(value, repeattimes):
    return [value] * repeattimes

@udf(
     returnType = ArrayType(StructType([
StructField("char", StringType(), False), StructField("count", IntegerType(), False)]))
)
def udf_maptype():
    return [{"char" : "char1", "count" : 10}]

@udf(returnType = ArrayType(StringType()))
def udf_tokenize(text):
    # Tokenize the text
    tokens = text.split(" ")
    '''Remove stop words'''
    stop_words = {"the", "is", "are", "and", "to", "of", "in"}
    tokens = [token for token in tokens if token not in stop_words]
    """Perform stemming"""
    stemmer = SnowballStemmer("english")
    tokens = [stemmer.stem(token) for token in tokens]

    return tokens
