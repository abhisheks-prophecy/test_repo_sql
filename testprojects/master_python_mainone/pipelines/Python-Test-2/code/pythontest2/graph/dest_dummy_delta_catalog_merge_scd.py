from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from pythontest2.config.ConfigStore import *
from pythontest2.udfs.UDFs import *

def dest_dummy_delta_catalog_merge_scd(spark: SparkSession, in0: DataFrame):
    if spark.catalog._jcatalog.tableExists(f"qa_database.testination_1"):
        from delta.tables import DeltaTable, DeltaMergeBuilder
        existingTable = DeltaTable.forName(spark, f"qa_database.testination_1")
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
"c_boolean" : "false", "c_struct.c_timestamp" : "staged_updates.c_struct.c_date"}
            )\
            .whenNotMatchedInsertAll()\
            .execute()
    else:
        in0.write\
            .format("delta")\
            .option("path", "dbfs/tmp/e2e/random1231234/dest_dummy_delta_catalog_merge_scd")\
            .mode("overwrite")\
            .saveAsTable(f"qa_database.testination_1")
