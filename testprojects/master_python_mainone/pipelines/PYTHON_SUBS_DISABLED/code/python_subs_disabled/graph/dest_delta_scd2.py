from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from python_subs_disabled.config.ConfigStore import *
from python_subs_disabled.udfs.UDFs import *

def dest_delta_scd2(spark: SparkSession, in0: DataFrame):
    from delta.tables import DeltaTable, DeltaMergeBuilder

    if DeltaTable.isDeltaTable(spark, "dbfs:/tmp/e2e/234234/dest_delta_scd2"):
        existingTable = DeltaTable.forPath(spark, "dbfs:/tmp/e2e/234234/dest_delta_scd2")
        updatesDF = in0.withColumn("c_boolean", lit("true")).withColumn("c_boolean", lit("true"))
        existingDF = existingTable.toDF()
        updateColumns = updatesDF.columns
        stagedUpdatesDF = updatesDF\
                              .join(existingDF, ["c_short"])\
                              .where((existingDF["c_boolean"] == lit("true")) & (existingDF["c_int"] != updatesDF["c_int"]))\
                              .select(*[updatesDF[val] for val in updateColumns])\
                              .withColumn("c_boolean", lit("false"))\
                              .withColumn("mergeKey", lit(None))\
                              .union(updatesDF.withColumn("mergeKey", concat("c_short")))
        existingTable\
            .alias("existingTable")\
            .merge(stagedUpdatesDF.alias("staged_updates"), concat(existingDF["c_short"]) == stagedUpdatesDF["mergeKey"])\
            .whenMatchedUpdate(
              condition = (existingDF["c_boolean"] == lit("true")) & (existingDF["c_int"] != stagedUpdatesDF["c_int"]),
              set = {
"c_boolean" : "false", "c_timestamp" : "staged_updates.c_date"}
            )\
            .whenNotMatchedInsertAll()\
            .execute()
    else:
        in0.write.format("delta").mode("overwrite").save("dbfs:/tmp/e2e/234234/dest_delta_scd2")
