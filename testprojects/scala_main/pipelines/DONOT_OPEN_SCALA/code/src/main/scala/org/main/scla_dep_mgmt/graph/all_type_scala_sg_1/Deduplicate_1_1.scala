package org.main.scla_dep_mgmt.graph.all_type_scala_sg_1

import io.prophecy.libs._
import org.main.scla_dep_mgmt.config.ConfigStore._
import org.main.scla_dep_mgmt.udfs.UDFs._
import org.main.scla_dep_mgmt.udfs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time._

object Deduplicate_1_1 {

  def apply(spark: SparkSession, in: DataFrame): DataFrame = {
    import org.apache.spark.sql.expressions.Window
    in.dropDuplicates(List("c -  boolean _  ", "c-string"))
  }

}