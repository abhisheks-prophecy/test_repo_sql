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

def registerUDFs(spark: SparkSession):
    spark.udf.register("squared", squared)
    spark.udf.register("factorial", factorial)
    spark.udf.register("random_string", random_string)
    spark.udf.register("udf_scipy_dependency", udf_scipy_dependency)

def squaredGenerator():
    initial = 10

    @udf(returnType = IntegerType())
    def func(input):
        input = int(input) if input is not None else 2

        return int(input) * int(input) * initial if input is not None else initial

    return func

squared = squaredGenerator()

def factorialGenerator():
    initial = 10

    @udf(returnType = IntegerType())
    def func(input):
        input = int(input) if input is not None else 2

        return int(input) * int(input) if input is not None else initial

    return func

factorial = factorialGenerator()

def random_stringGenerator():
    initial = 10

    @udf(returnType = StringType())
    def func(length, extra_characters=""):
        import string, random
        length = int(length) if length is not None else 2

        return "".join(
            [
              random.choice(string.ascii_lowercase + string.ascii_uppercase + string.digits + extra_characters)
              for _ in range(length)
            ]
        )

    return func

random_string = random_stringGenerator()

def udf_scipy_dependencyGenerator():
    initial = 10

    @udf(returnType = StringType())
    def func():
        from scipy.special import cbrt
        cb = cbrt([27, 64])

        return str(cb[0])

    return func

udf_scipy_dependency = udf_scipy_dependencyGenerator()
