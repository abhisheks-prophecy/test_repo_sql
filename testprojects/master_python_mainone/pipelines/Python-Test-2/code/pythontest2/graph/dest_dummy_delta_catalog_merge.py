from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from pythontest2.config.ConfigStore import *
from pythontest2.udfs.UDFs import *

def dest_dummy_delta_catalog_merge(spark: SparkSession, in0: DataFrame):
    if spark.catalog._jcatalog.tableExists(f"qa_database.testination_1"):
        from delta.tables import DeltaTable, DeltaMergeBuilder
        DeltaTable\
            .forName(spark, f"qa_database.testination_1")\
            .alias("target")\
            .merge(in0.alias("source"), (col("source.c_int") == col("target.c_int")))\
            .whenMatchedUpdateAll(condition = col("source.c_boolean").eqNullSafe(lit(True)))\
            .whenNotMatchedInsertAll()\
            .execute()
    else:
        in0.write\
            .format("delta")\
            .option("path", "dbfs:/tmp/e2e/random1234/dest_dummy_delta_catalog_merge")\
            .mode("overwrite")\
            .saveAsTable(f"qa_database.testination_1")
