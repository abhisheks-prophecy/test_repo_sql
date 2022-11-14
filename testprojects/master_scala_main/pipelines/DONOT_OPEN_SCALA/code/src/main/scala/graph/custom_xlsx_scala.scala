package graph

import io.prophecy.libs._
import config.ConfigStore._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time._

object custom_xlsx_scala {

  def apply(spark: SparkSession): DataFrame =
    spark.read
      .format("excel")
      .option("header",      "true")
      .option("inferSchema", "true")
      .load("dbfs:/FileStore/Users/abhinav/test_1.xlsx")

}
