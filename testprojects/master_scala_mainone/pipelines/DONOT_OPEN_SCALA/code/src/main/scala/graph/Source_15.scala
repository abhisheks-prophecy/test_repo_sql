package graph

import io.prophecy.libs._
import config.ConfigStore._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time._

object Source_15 {

  def apply(spark: SparkSession): DataFrame =
    spark.read.table("qa_database.test_catalog_source")

}
