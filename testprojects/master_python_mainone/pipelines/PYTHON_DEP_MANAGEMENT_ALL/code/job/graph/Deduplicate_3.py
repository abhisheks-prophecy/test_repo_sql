from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Deduplicate_3(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "row_number",
          row_number()\
            .over(Window\
            .partitionBy("c_array-- boolean ")\
            .orderBy(concat(col("p_short"), col("p_int")).asc(), col("`c_array-- boolean `").desc())\
            .rowsBetween(Window.unboundedPreceding, Window.currentRow))
        )\
        .withColumn(
          "count",
          count("*")\
            .over(Window\
            .partitionBy("c_array-- boolean ")\
            .orderBy(concat(col("p_short"), col("p_int")).asc(), col("`c_array-- boolean `").desc())\
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))
        )\
        .filter(col("row_number") == col("count"))\
        .drop("row_number")\
        .drop("count")
