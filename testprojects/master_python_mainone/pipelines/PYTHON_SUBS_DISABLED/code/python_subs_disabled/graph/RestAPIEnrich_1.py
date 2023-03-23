from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from python_subs_disabled.config.ConfigStore import *
from python_subs_disabled.udfs.UDFs import *

def RestAPIEnrich_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn(
        "api_output",
        get_rest_api(
          to_json(struct(lit("GET").alias("method"), lit("https://www.boredapi.com/api/activity").alias("url"))),
          lit("")
        )
    )
