from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from pythontest2.config.ConfigStore import *
from pythontest2.udfs.UDFs import *

def dest_dummy_delta_scd2(spark: SparkSession, in0: DataFrame):
    from delta.tables import DeltaTable, DeltaMergeBuilder

    if DeltaTable.isDeltaTable(spark, "dbfs:/tmp/e2e/random123/dest_dummy_delta_scd2"):
        existingTable = DeltaTable.forPath(spark, "dbfs:/tmp/e2e/random123/dest_dummy_delta_scd2")
        updatesDF = in0.withColumn("c_boolean", lit("true")).withColumn("c_boolean", lit("true"))
        existingDF = existingTable.toDF()
        updateColumns = updatesDF.columns
        stagedUpdatesDF = updatesDF\
                              .join(existingDF, ["c_int"])\
                              .where(
                                (
                                  (existingDF["c_boolean"] == lit("true"))
                                  & (
                                    existingDF["c_decimal"]
                                    != updatesDF["c_decimal"]
                                  )
                                )
                              )\
                              .select(*[updatesDF[val] for val in updateColumns])\
                              .withColumn("c_boolean", lit("false"))\
                              .withColumn("mergeKey", lit(None))\
                              .union(updatesDF.withColumn("mergeKey", concat("c_int")))
        existingTable\
            .alias("existingTable")\
            .merge(stagedUpdatesDF.alias("staged_updates"), concat(existingDF["c_int"]) == stagedUpdatesDF["mergeKey"])\
            .whenMatchedUpdate(
              condition = (existingDF["c_boolean"] == lit("true")) & (existingDF["c_decimal"] != stagedUpdatesDF["c_decimal"]),
              set = {
"c_boolean" : "false", "c_struct.c_date" : "staged_updates.c_date"}
            )\
            .whenNotMatchedInsertAll()\
            .execute()
    else:
        in0.write.format("delta").mode("overwrite").save("dbfs:/tmp/e2e/random123/dest_dummy_delta_scd2")
