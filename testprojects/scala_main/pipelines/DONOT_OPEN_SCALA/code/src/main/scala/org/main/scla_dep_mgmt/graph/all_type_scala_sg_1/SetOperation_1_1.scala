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

object SetOperation_1_1 {

  def apply(spark: SparkSession, in0: DataFrame, in1: DataFrame): DataFrame =
    in0.unionAll(in1)

}