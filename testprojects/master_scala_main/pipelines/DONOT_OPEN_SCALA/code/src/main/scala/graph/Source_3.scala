package graph

import io.prophecy.libs._
import config.ConfigStore._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time._

object Source_3 {

  def apply(spark: SparkSession): DataFrame = {
    import org.apache.avro.Schema
    var reader = spark.read.format("avro")
    reader = reader
    reader
      .load("dbfs:/Prophecy/qa_data/avro/CustomersDatasetInput.avro")
      .cache()
  }

}
