package org.scala.sub_graph_src1.graph.Subgraph_1

import io.prophecy.libs._
import com.main.sub_graph_src1.config.ConfigStore._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object src_csv_all_type_no_partition_1 {

  def apply(spark: SparkSession): DataFrame = {
    import org.apache.avro.Schema
    var reader = spark.read.format("avro")
    reader = reader
    reader.load("dbfs:/Prophecy/qa_data/avro/CustomersDatasetInput.avro")
  }

}
