package org.main.scla_dep_mgmt.udfs

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object UDFs extends Serializable {
  var int_value    = 10
  var string_value = "string value"
  def registerUDFs(spark: SparkSession) = {}
}
