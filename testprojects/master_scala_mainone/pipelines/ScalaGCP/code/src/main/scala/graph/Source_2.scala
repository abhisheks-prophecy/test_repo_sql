package graph

import io.prophecy.libs._
import config.ConfigStore._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Source_2 {

  def apply(spark: SparkSession): DataFrame =
    spark.read
      .format("parquet")
      .load("dbfs:/Prophecy/qa_data/parquet/all_type_no_partition")
      .cache()

}
