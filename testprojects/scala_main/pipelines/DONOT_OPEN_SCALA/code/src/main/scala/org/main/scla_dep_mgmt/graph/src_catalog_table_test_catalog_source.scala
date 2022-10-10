package org.main.scla_dep_mgmt.graph

import io.prophecy.libs._
import org.main.scla_dep_mgmt.config.ConfigStore._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time._

object src_catalog_table_test_catalog_source {

  def apply(spark: SparkSession): DataFrame =
    spark.read.table("qa_database.test_catalog_source")

}
