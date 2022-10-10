package org.main.scla_dep_mgmt.graph

import io.prophecy.libs._
import org.main.scla_dep_mgmt.config.ConfigStore._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object src_orc_all_type_no_partition {

  def apply(spark: SparkSession): DataFrame =
    spark.read
      .format("orc")
      .load("dbfs:/Prophecy/qa_data/orc/all_type_no_partition")

}
