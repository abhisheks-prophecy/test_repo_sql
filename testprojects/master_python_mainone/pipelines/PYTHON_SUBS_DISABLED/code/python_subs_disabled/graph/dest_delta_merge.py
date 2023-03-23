from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from python_subs_disabled.config.ConfigStore import *
from python_subs_disabled.udfs.UDFs import *

def dest_delta_merge(spark: SparkSession, in0: DataFrame):
    from delta.tables import DeltaTable, DeltaMergeBuilder

    if DeltaTable.isDeltaTable(spark, "dbfs:/tmp/e2e/234324/dest_delta_merge123"):
        matched_expr = {}
        matched_expr["c_string"] = concat(col("source.c_string"), col("source.c_int"))
        matched_expr["c_int"] = ((col("source.c_int") + col("source.c_int")) / lit(2))
        DeltaTable\
            .forPath(spark, "dbfs:/tmp/e2e/234324/dest_delta_merge123")\
            .alias("target")\
            .merge(in0.alias("source"), (col("source.c_increasing") == col("target.c_increasing")))\
            .whenMatchedUpdate(condition = (col("source.c_int") == lit(35)), set = matched_expr)\
            .whenNotMatchedInsertAll()\
            .execute()
    else:
        in0.write.format("delta").mode("overwrite").save("dbfs:/tmp/e2e/234324/dest_delta_merge123")
