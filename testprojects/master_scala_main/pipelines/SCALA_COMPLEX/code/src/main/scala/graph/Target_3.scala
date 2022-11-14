package graph

import io.prophecy.libs._
import config.ConfigStore._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Target_3 {

  def apply(spark: SparkSession, in: DataFrame): Unit =
    in.write
      .format("hive")
      .option("fileFormat", "parquet")
      .mode("overwrite")
      .saveAsTable("qa_database.test_catalog_destination_1")

}
